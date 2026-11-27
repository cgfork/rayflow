use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

const GRAPH: &str = "dddddddd-0000-4000-8000-000000000001";
const NODE: &str = "dddddddd-0000-4000-8000-000000000002";

fn run_task(root: &Path, script: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_runlua"))
        .args([script.as_os_str(), "daily".as_ref()])
        .env("RAYFLOW_JOB_FILE", root.join("job.json"))
        .env("RAYFLOW_GRAPH_FILE", root.join("graph.json"))
        .env("RAYFLOW_INPUT_DIR", root.join("inputs"))
        .env("RAYFLOW_OUTPUT_DIR", root.join("outputs"))
        .env("RAYFLOW_ASSET_DIR", root.join("asset"))
        .output()
        .unwrap()
}

#[test]
fn runs_a_pinned_script_asset_and_copies_binary_artifacts() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("asset")).unwrap();
    fs::create_dir(root.join("outputs")).unwrap();
    fs::create_dir_all(root.join("inputs/bundle")).unwrap();
    fs::write(root.join("inputs/bundle/content.bin"), [0, 128, 255]).unwrap();
    fs::write(
        root.join("inputs/bundle/.rayflow-directory.json"),
        r#"{"version":1,"entries":[{"path":"content.bin","kind":"file","size":3}]}"#,
    )
    .unwrap();
    let script = root.join("asset/task.lua");
    fs::write(
        &script,
        r#"
        assert(arg[1] == "daily" and ... == "daily")
        assert(rayflow.job.custom == "task metadata")
        assert(rayflow.graph.version == 3 and rayflow.node.name == "process")
        assert(rayflow.asset_path("task.lua") == arg[0])
        assert(rayflow.input_path("optional.txt") == nil)
        assert(not pcall(rayflow.input_path, "bundle", "../outside"))
        local entries = rayflow.list_input("bundle")
        assert(#entries == 1 and entries[1].path == "content.bin")
        local input = assert(io.open(rayflow.input_path("bundle", entries[1].path), "rb"))
        local output = assert(io.open(rayflow.output_path("result", "sub/copy.bin"), "wb"))
        output:write(input:read("*a"))
        output:close()
        input:close()
    "#,
    )
    .unwrap();
    let job = |version: u64| {
        serde_json::json!({
            "node_run": {"graph_id": GRAPH, "node_id": NODE, "version": version},
            "run": {"version": 4},
            "custom": "task metadata"
        })
    };
    fs::write(root.join("job.json"), serde_json::to_vec(&job(3)).unwrap()).unwrap();
    let graph = serde_json::json!({
        "id": GRAPH, "version": 3, "name": "example", "node_count": 1, "edges": [],
        "nodes": [{
            "id": NODE, "name": "process", "node_type": "docker",
            "inputs": [
                {"file_name":"bundle", "source":"package.bundle", "required":true},
                {"file_name":"optional.txt", "source":"package.optional.txt", "required":false}
            ],
            "outputs": [{"file_name":"result", "kind":"Directory"}],
            "assets": [{"file_name":"task.lua", "asset_id":"dddddddd-0000-4000-8000-000000000003"}]
        }]
    });
    fs::write(root.join("graph.json"), serde_json::to_vec(&graph).unwrap()).unwrap();
    let result = run_task(root, &script);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(
        fs::read(root.join("outputs/result/sub/copy.bin")).unwrap(),
        [0, 128, 255]
    );

    fs::write(root.join("job.json"), serde_json::to_vec(&job(4)).unwrap()).unwrap();
    let failure = run_task(root, &script);
    assert!(!failure.status.success());
    assert!(
        String::from_utf8_lossy(&failure.stderr)
            .contains("graph does not match the pinned node run")
    );
}
