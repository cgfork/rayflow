use std::{collections::HashMap, sync::LazyLock};

use cel_core::{CelType, CompileError, Env, MapActivation, Program, Value};

const ANNOTATIONS: &str = "annotations";
const MAX_EXPRESSION_BYTES: usize = 255;

static ENV: LazyLock<Env> = LazyLock::new(|| {
    Env::with_standard_library()
        .with_variable(ANNOTATIONS, CelType::map(CelType::String, CelType::String))
});

#[derive(Debug, thiserror::Error)]
pub enum AnnotationMatchError {
    #[error("expression must be at most {MAX_EXPRESSION_BYTES} bytes")]
    TooLong,
    #[error("expression must return a boolean")]
    NonBoolean,
    #[error(transparent)]
    Compile(#[from] CompileError),
}

/// A CEL predicate over the run's string-to-string annotation map.
pub struct AnnotationMatch(Program);

impl AnnotationMatch {
    /// Compiles a predicate or returns `None` when the expression is blank.
    pub fn compile(source: &str) -> Result<Option<Self>, AnnotationMatchError> {
        if source.len() > MAX_EXPRESSION_BYTES {
            return Err(AnnotationMatchError::TooLong);
        }
        let source = source.trim();
        if source.is_empty() {
            return Ok(None);
        }
        let ast = ENV.compile(source)?;
        if ast.result_type() != Some(&CelType::Bool) {
            return Err(AnnotationMatchError::NonBoolean);
        }
        Ok(Some(Self(ENV.program(&ast)?)))
    }

    /// Matches only when the predicate evaluates successfully to `true`.
    pub fn matches(&self, annotations: &HashMap<String, String>) -> bool {
        let mut activation = MapActivation::new();
        activation.insert(
            ANNOTATIONS,
            Value::map(
                annotations
                    .iter()
                    .map(|(key, value)| (key.as_str(), value.as_str())),
            ),
        );
        matches!(self.0.eval(&activation), Value::Bool(true))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checks_the_annotation_map_and_boolean_result() {
        assert!(AnnotationMatch::compile(" \t").unwrap().is_none());
        let at_limit = format!("'{}x' != ''", "é".repeat(123));
        assert_eq!(at_limit.len(), MAX_EXPRESSION_BYTES);
        assert!(AnnotationMatch::compile(&at_limit).is_ok());
        assert!(matches!(
            AnnotationMatch::compile(&format!("{at_limit} ")),
            Err(AnnotationMatchError::TooLong)
        ));
        assert!(matches!(
            AnnotationMatch::compile("annotations.priority"),
            Err(AnnotationMatchError::NonBoolean)
        ));
        assert!(AnnotationMatch::compile("priority == 'high'").is_err());
        assert!(matches!(
            AnnotationMatch::compile(r#"annotation.priority == "high""#),
            Err(AnnotationMatchError::Compile(CompileError::Check(_)))
        ));
        assert!(AnnotationMatch::compile("$.priority").is_err());
    }

    #[test]
    fn matches_priority_using_the_editor_example() {
        let expression = r#"has(annotations.priority) && annotations.priority == "high""#;
        let matcher = AnnotationMatch::compile(expression).unwrap().unwrap();
        assert!(matcher.matches(&HashMap::from([("priority".into(), "high".into())])));
        assert!(!matcher.matches(&HashMap::from([("priority".into(), "low".into())])));
        assert!(!matcher.matches(&HashMap::from([("dataset".into(), "filetest".into())])));
    }

    #[test]
    fn missing_unguarded_annotation_does_not_match() {
        let unguarded = AnnotationMatch::compile("annotations.priority == 'high'")
            .unwrap()
            .unwrap();
        assert!(!unguarded.matches(&HashMap::new()));
    }
}
