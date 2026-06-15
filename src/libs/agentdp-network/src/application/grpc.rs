pub(crate) fn content_type_is_grpc(value: &str) -> bool {
    value
        .split(';')
        .next()
        .is_some_and(|content_type| content_type.trim().eq_ignore_ascii_case("application/grpc"))
}

#[cfg(test)]
mod tests {
    use super::content_type_is_grpc;

    #[test]
    fn recognizes_grpc_content_type_with_parameters() {
        assert!(content_type_is_grpc("application/grpc; charset=utf-8"));
        assert!(content_type_is_grpc(" Application/GRPC "));
        assert!(!content_type_is_grpc("application/grpc+proto"));
    }
}
