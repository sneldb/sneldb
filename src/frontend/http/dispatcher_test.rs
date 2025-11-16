use crate::logging::init_for_tests;
use hyper::http::{HeaderMap, HeaderValue};

use super::dispatcher::extract_client_ip_from_header_map;

/// Helper function to create test headers
fn create_test_headers(headers: Vec<(&'static str, &'static str)>) -> HeaderMap {
    let mut header_map = HeaderMap::new();
    for (name, value) in headers {
        if let Ok(header_value) = HeaderValue::from_str(value) {
            header_map.insert(name, header_value);
        }
    }
    header_map
}

#[tokio::test]
async fn test_extract_client_ip_from_x_forwarded_for() {
    init_for_tests();

    let headers = create_test_headers(vec![("X-Forwarded-For", "192.168.1.100")]);
    let client_ip = extract_client_ip_from_header_map(&headers);

    assert_eq!(client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_extract_client_ip_from_x_forwarded_for_with_multiple_ips() {
    init_for_tests();

    // X-Forwarded-For can contain multiple IPs (proxy chain)
    // Should take the first one (original client)
    let headers = create_test_headers(vec![(
        "X-Forwarded-For",
        "192.168.1.100, 10.0.0.1, 172.16.0.1",
    )]);
    let client_ip = extract_client_ip_from_header_map(&headers);

    assert_eq!(client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_extract_client_ip_from_x_real_ip() {
    init_for_tests();

    let headers = create_test_headers(vec![("X-Real-IP", "192.168.1.200")]);
    let client_ip = extract_client_ip_from_header_map(&headers);

    assert_eq!(client_ip, Some("192.168.1.200".to_string()));
}

#[tokio::test]
async fn test_extract_client_ip_prefers_x_forwarded_for_over_x_real_ip() {
    init_for_tests();

    // X-Forwarded-For should be preferred over X-Real-IP
    let headers = create_test_headers(vec![
        ("X-Forwarded-For", "192.168.1.100"),
        ("X-Real-IP", "192.168.1.200"),
    ]);
    let client_ip = extract_client_ip_from_header_map(&headers);

    assert_eq!(client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_extract_client_ip_returns_none_when_no_headers() {
    init_for_tests();

    let headers = HeaderMap::new();
    let client_ip = extract_client_ip_from_header_map(&headers);

    assert_eq!(client_ip, None);
}

#[tokio::test]
async fn test_extract_client_ip_handles_ipv6() {
    init_for_tests();

    let headers = create_test_headers(vec![(
        "X-Forwarded-For",
        "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
    )]);
    let client_ip = extract_client_ip_from_header_map(&headers);

    assert_eq!(
        client_ip,
        Some("2001:0db8:85a3:0000:0000:8a2e:0370:7334".to_string())
    );
}

#[tokio::test]
async fn test_extract_client_ip_trims_whitespace() {
    init_for_tests();

    // X-Forwarded-For with whitespace
    let headers = create_test_headers(vec![("X-Forwarded-For", "  192.168.1.100  ")]);
    let client_ip = extract_client_ip_from_header_map(&headers);

    assert_eq!(client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_extract_client_ip_handles_comma_separated_with_whitespace() {
    init_for_tests();

    // X-Forwarded-For with whitespace around comma
    let headers = create_test_headers(vec![("X-Forwarded-For", "  192.168.1.100  ,  10.0.0.1  ")]);
    let client_ip = extract_client_ip_from_header_map(&headers);

    assert_eq!(client_ip, Some("192.168.1.100".to_string()));
}

#[tokio::test]
async fn test_extract_client_ip_empty_x_forwarded_for() {
    init_for_tests();

    let headers = create_test_headers(vec![("X-Forwarded-For", "")]);
    let client_ip = extract_client_ip_from_header_map(&headers);

    // Empty string should result in None (split(',').next() returns Some("") which is trimmed)
    assert_eq!(client_ip, None);
}

#[tokio::test]
async fn test_extract_client_ip_with_proxy_chain() {
    init_for_tests();

    // Real-world scenario: client -> proxy1 -> proxy2 -> server
    let headers = create_test_headers(vec![(
        "X-Forwarded-For",
        "192.168.1.100, 10.0.0.1, 172.16.0.1",
    )]);
    let client_ip = extract_client_ip_from_header_map(&headers);

    // Should extract the first IP (original client)
    assert_eq!(client_ip, Some("192.168.1.100".to_string()));
}
