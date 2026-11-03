use graph::{Page, PageRequest};

#[test]
fn page_requests_validate_limits() {
    assert_eq!(PageRequest::default().limit(), 50);
    assert_eq!(PageRequest::default().before(), None);
    for limit in [0, 201, usize::MAX] {
        assert!(PageRequest::new(None, limit).is_err());
    }
    for limit in [1, 50, 200] {
        assert_eq!(PageRequest::new(Some(0), limit).unwrap().limit(), limit);
    }
}

#[test]
fn pages_use_exclusive_cursors_and_report_only_existing_next_pages() {
    let entries = [(9, "new"), (5, "middle"), (2, "old")];
    let first = PageRequest::new(None, 2).unwrap().select(entries);
    assert_eq!(first.items, ["new", "middle"]);
    assert_eq!(first.next_cursor, Some(5));
    let next = PageRequest::new(first.next_cursor, 2)
        .unwrap()
        .select(entries);
    assert_eq!(next.items, ["old"]);
    assert_eq!(next.next_cursor, None);
    let exact = PageRequest::new(None, 3).unwrap().select(entries);
    assert_eq!(exact.next_cursor, None);
    let empty = PageRequest::new(Some(0), 2).unwrap().select(entries);
    assert!(empty.items.is_empty());
    assert_eq!(empty.next_cursor, None);
    let mapped = first.map(str::to_owned);
    let json = serde_json::to_value(&mapped).unwrap();
    assert_eq!(json["next_cursor"], 5);
    assert_eq!(
        serde_json::from_value::<Page<String>>(json).unwrap(),
        mapped
    );
}
