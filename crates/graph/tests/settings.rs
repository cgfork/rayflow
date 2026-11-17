use std::time::Duration;

use graph::{
    Diff,
    types::{DAG, Input, Kind, Node, NodeSettings, Output, Resource},
    validator::{ValidationError, WorkflowValidationError, validate_workflow},
};
use serde_json::json;
use uuid::Uuid;

fn node(name: &str) -> Node {
    Node {
        id: Uuid::new_v4(),
        name: name.into(),
        node_type: "docker".into(),
        settings: NodeSettings {
            config: json!({"image": "alpine:3.21", "command": ["echo", "hello"]}),
            resource: Resource {
                cpu_cores: 2,
                memory_bytes: 536_870_912,
            },
            timeout: Duration::from_secs(10),
        },
        inputs: Vec::new(),
        outputs: Vec::new(),
    }
}

fn workflow() -> DAG {
    DAG {
        name: "workflow".into(),
        nodes: vec![node("start")],
        ..DAG::default()
    }
}

#[test]
fn workflow_validation_retains_structural_errors() {
    let mut graph = workflow();
    let id = graph.nodes[0].id;
    graph.edges.push((id, id));
    assert_eq!(
        validate_workflow(&graph),
        Err(WorkflowValidationError::Structure(ValidationError::Cycle))
    );
}

#[test]
fn node_settings_round_trip_json_and_duration_strings() {
    let original = node("start");
    let mut value = serde_json::to_value(&original).unwrap();
    assert!(value.get("config").is_none());
    assert_eq!(
        value["settings"]["config"]["command"],
        json!(["echo", "hello"])
    );
    assert_eq!(value["settings"]["resource"]["cpu_cores"], 2);
    assert_eq!(value["settings"]["timeout"], "10s");
    assert_eq!(
        serde_json::from_value::<Node>(value.clone()).unwrap(),
        original
    );
    for (duration, milliseconds) in [
        ("250ms", 250),
        ("1.5s", 1500),
        ("2m", 120_000),
        ("PT10S", 10_000),
    ] {
        value["settings"]["timeout"] = json!(duration);
        let decoded: Node = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(decoded.settings.timeout_ms(), Some(milliseconds));
    }
    value.as_object_mut().unwrap().remove("settings");
    let legacy: Node = serde_json::from_value(value).unwrap();
    assert_eq!(legacy.settings, NodeSettings::default());
    assert!(
        validate_workflow(&DAG {
            nodes: vec![legacy],
            ..workflow()
        })
        .is_err()
    );
}

#[test]
fn publishing_requires_complete_valid_runnable_settings() {
    let valid = workflow();
    assert!(validate_workflow(&valid).is_ok());
    let original = valid.nodes[0].settings.clone();
    for config in [
        json!(null),
        json!([]),
        json!({}),
        json!({"image": " "}),
        json!({"image": "alpine", "command": "echo"}),
        json!({"image": "alpine", "env": [123]}),
        json!({"image": "alpine", "working_dir": 123}),
    ] {
        let mut invalid = valid.clone();
        invalid.nodes[0].settings.config = config;
        assert!(validate_workflow(&invalid).is_err());
    }
    for settings in [
        NodeSettings {
            resource: Resource {
                cpu_cores: 0,
                ..original.resource.clone()
            },
            ..original.clone()
        },
        NodeSettings {
            resource: Resource {
                memory_bytes: 0,
                ..original.resource.clone()
            },
            ..original.clone()
        },
        NodeSettings {
            resource: Resource {
                memory_bytes: u64::MAX,
                ..original.resource.clone()
            },
            ..original.clone()
        },
        NodeSettings {
            timeout: Duration::ZERO,
            ..original.clone()
        },
        NodeSettings {
            timeout: Duration::from_nanos(1),
            ..original.clone()
        },
        NodeSettings {
            timeout: Duration::from_millis(u64::MAX),
            ..original.clone()
        },
    ] {
        let mut invalid = valid.clone();
        invalid.nodes[0].settings = settings;
        assert!(validate_workflow(&invalid).is_err());
    }
}

#[test]
fn settings_and_resource_diffs_include_each_field() {
    let original = node("start");
    for settings in [
        NodeSettings {
            config: json!({"image": "busybox"}),
            ..original.settings.clone()
        },
        NodeSettings {
            resource: Resource {
                cpu_cores: 3,
                ..original.settings.resource.clone()
            },
            ..original.settings.clone()
        },
        NodeSettings {
            resource: Resource {
                memory_bytes: 1024,
                ..original.settings.resource.clone()
            },
            ..original.settings.clone()
        },
        NodeSettings {
            timeout: Duration::from_secs(20),
            ..original.settings.clone()
        },
    ] {
        assert!(original.settings.diff(&settings).is_some());
        let changed = Node {
            settings,
            ..original.clone()
        };
        assert!(original.diff(&changed).is_some());
    }
    assert!(original.settings.diff(&original.settings).is_none());
    assert!(
        original
            .settings
            .resource
            .diff(&Resource::default())
            .is_some()
    );
    assert!(
        original
            .settings
            .resource
            .diff(&original.settings.resource)
            .is_none()
    );
}

#[test]
fn input_bindings_require_declared_upstream_outputs() {
    let mut producer = node("producer");
    producer.outputs.push(Output {
        file_name: "result.json".into(),
        kind: Kind::File,
        multiple: false,
    });
    let intermediate = node("middle");
    let mut consumer = node("consumer");
    consumer.inputs.push(Input {
        file_name: "local.json".into(),
        source: "producer.result.json".into(),
        required: false,
        format: None,
    });
    let valid = DAG {
        edges: vec![
            (producer.id, intermediate.id),
            (intermediate.id, consumer.id),
        ],
        nodes: vec![producer, intermediate, consumer],
        ..workflow()
    };
    assert!(validate_workflow(&valid).is_ok());
    for source in ["", "producer.missing", "consumer.result.json"] {
        let mut invalid = valid.clone();
        invalid.nodes[2].inputs[0].source = source.into();
        assert!(validate_workflow(&invalid).is_err());
    }
    let mut reversed = valid.clone();
    reversed.nodes[0].inputs = valid.nodes[2].inputs.clone();
    reversed.nodes[2].inputs.clear();
    assert!(validate_workflow(&reversed).is_err());
    let mut duplicate = valid;
    duplicate.nodes[1].name = duplicate.nodes[0].name.clone();
    assert!(validate_workflow(&duplicate).is_err());
}
