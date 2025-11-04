use super::errors::ShowError;

#[test]
fn constructs_error_with_message() {
    let error = ShowError::new("boom");
    assert_eq!(error.message(), "boom");
}

#[test]
fn displays_message_via_display_trait() {
    let error = ShowError::new("display me");
    assert_eq!(error.to_string(), "display me");
}

#[test]
fn implements_std_error_trait() {
    let error = ShowError::new("std-error");
    let dyn_err: &dyn std::error::Error = &error;
    assert_eq!(dyn_err.to_string(), "std-error");
}
