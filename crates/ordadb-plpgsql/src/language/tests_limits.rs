
#[test]
fn nested_vm_frames_share_one_raii_retained_memory_grant() {
    let program = compile_with_arguments("BEGIN RETURN $1; END;", &["payload".to_owned()])
        .expect("compile routine");
    let limits = ResourceLimits {
        max_cursor_bytes: 256 * 1024,
        ..ResourceLimits::default()
    };
    let grant = VmMemoryGrant::new(limits.max_cursor_bytes).expect("memory grant");
    let payload = Value::Text("x".repeat(150 * 1024));
    let mut first_host = Host { cancelled: false };
    let first = VmMachine::new_with_memory_grant(
        &program,
        &mut first_host,
        std::slice::from_ref(&payload),
        limits,
        grant.clone(),
    )
    .expect("first frame");
    let first_bytes = grant.current_bytes();
    assert!(first_bytes > 150 * 1024);

    let mut second_host = Host { cancelled: false };
    let error = match VmMachine::new_with_memory_grant(
        &program,
        &mut second_host,
        std::slice::from_ref(&payload),
        limits,
        grant.clone(),
    ) {
        Ok(_) => panic!("nested frame must share the hard limit"),
        Err(error) => error,
    };
    assert_eq!(error.sql_state, "53200");
    assert_eq!(grant.current_bytes(), first_bytes);
    drop(first);
    assert_eq!(grant.current_bytes(), 0);

    let replacement = VmMachine::new_with_memory_grant(
        &program,
        &mut second_host,
        &[payload],
        limits,
        grant.clone(),
    )
    .expect("released bytes are reusable");
    drop(replacement);
    assert_eq!(grant.current_bytes(), 0);
    assert!(grant.peak_bytes() <= grant.hard_limit_bytes());
}

#[test]
fn completed_output_holds_its_raii_reservation_until_drop() {
    let program = compile_with_arguments("BEGIN RETURN $1; END;", &["payload".to_owned()])
        .expect("compile routine");
    let limits = ResourceLimits {
        max_cursor_bytes: 256 * 1024,
        ..ResourceLimits::default()
    };
    let grant = VmMemoryGrant::new(limits.max_cursor_bytes).expect("memory grant");
    let mut host = Host { cancelled: false };
    let mut machine = VmMachine::new_with_memory_grant(
        &program,
        &mut host,
        &[Value::Text("x".repeat(64 * 1024))],
        limits,
        grant.clone(),
    )
    .expect("create VM");
    let VmRunState::Complete(output) = machine.resume(&mut host, None).expect("complete VM")
    else {
        panic!("routine unexpectedly yielded SQL");
    };
    assert!(grant.current_bytes() > 64 * 1024);
    drop(machine);
    assert!(grant.current_bytes() > 64 * 1024);
    drop(output);
    assert_eq!(grant.current_bytes(), 0);
}

#[test]
fn cancellation_and_runtime_memory_errors_release_the_shared_grant() {
    let program = compile_with_arguments(
        "BEGIN RETURN NEXT $1; RETURN NEXT $1; END;",
        &["payload".to_owned()],
    )
    .expect("compile routine");
    let limits = ResourceLimits {
        max_cursor_bytes: 192 * 1024,
        ..ResourceLimits::default()
    };
    let payload = Value::Text("x".repeat(100 * 1024));

    let cancelled_grant = VmMemoryGrant::new(limits.max_cursor_bytes).expect("memory grant");
    let mut cancelled_host = Host { cancelled: false };
    let mut cancelled = VmMachine::new_with_memory_grant(
        &program,
        &mut cancelled_host,
        std::slice::from_ref(&payload),
        limits,
        cancelled_grant.clone(),
    )
    .expect("create cancelled VM");
    cancelled_host.cancelled = true;
    assert_eq!(
        cancelled
            .resume(&mut cancelled_host, None)
            .expect_err("cancel VM")
            .sql_state,
        "57014"
    );
    assert_eq!(cancelled_grant.current_bytes(), 0);

    let limited_grant = VmMemoryGrant::new(limits.max_cursor_bytes).expect("memory grant");
    let mut limited_host = Host { cancelled: false };
    let mut limited = VmMachine::new_with_memory_grant(
        &program,
        &mut limited_host,
        &[payload],
        limits,
        limited_grant.clone(),
    )
    .expect("create limited VM");
    assert_eq!(
        limited
            .resume(&mut limited_host, None)
            .expect_err("runtime memory limit")
            .sql_state,
        "53200"
    );
    assert_eq!(limited_grant.current_bytes(), 0);
}
