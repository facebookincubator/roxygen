mod network_tests {
    use roxygen_axum_tower_fixture::Event;
    use roxygen_axum_tower_fixture::FrameKind;
    use roxygen_axum_tower_fixture::Protocol;
    use roxygen_axum_tower_fixture::Report;
    use roxygen_axum_tower_fixture::Scenario;
    use roxygen_axum_tower_fixture::VerifierFault;
    use roxygen_axum_tower_fixture::run;

    fn qualified(protocol: Protocol, scenario: Scenario) -> Report {
        let report = run(protocol, scenario)
            .unwrap_or_else(|error| panic!("{protocol:?}/{scenario:?}: bootstrap: {error}"));
        if let Err(error) = report.verify() {
            panic!("{protocol:?}/{scenario:?}: {error}\n{report:#?}");
        }
        report
    }

    #[test]
    fn public_network_streaming() {
        for protocol in [Protocol::Http1, Protocol::Http2] {
            let mut report = qualified(protocol, Scenario::Streaming);
            assert_eq!(report.observations.upload.bytes(), b"public-stream");
            assert_eq!(report.observations.download.bytes(), b"public-stream");
            assert!(report.observations.first_response_before_upload_release);
            assert_eq!(
                report.observations.download.order.last(),
                Some(&FrameKind::Trailers),
            );
            assert_eq!(
                report.observations.request_version,
                Some(protocol.version())
            );
            assert_eq!(
                report.observations.response_version,
                Some(protocol.version())
            );
            assert!(matches!(&report.phases.client_exchange, Some(Ok(()))));
            assert!(matches!(&report.phases.server_exchange, Some(Ok(()))));
            assert!(report.phases.client_submission_retained_until_shutdown);
            assert_eq!(report.progress.nonempty_prefix_frames, b"public-".len());
            report
                .verify_rejects(VerifierFault::PrematurePrefix)
                .expect("incomplete prefix must be rejected");
            report
                .verify_rejects(VerifierFault::ProjectionRun)
                .expect("unrelated Run failure must be rejected");
            report
                .verify_rejects(VerifierFault::MissingClientSubmission)
                .expect("premature submission-handle loss must be rejected");

            let mut failed_body = qualified(protocol, Scenario::BodyError);
            assert_eq!(
                failed_body.observations.download.bytes(),
                b"response-prefix"
            );
            assert!(failed_body.observations.response_error.is_some());
            assert!(!failed_body.observations.download.eof);
            assert!(matches!(&failed_body.phases.client_exchange, Some(Ok(()))));
            assert!(failed_body.phases.client_submission_retained_until_shutdown);
            assert_eq!(
                failed_body.progress.nonempty_prefix_frames,
                b"response-prefix".len()
            );
            for fault in [
                VerifierFault::ReadTimeoutBody,
                VerifierFault::ProtocolErrorBody,
                VerifierFault::ProjectionRun,
                VerifierFault::ReadTimeoutRun,
                VerifierFault::ProtocolErrorRun,
                VerifierFault::PrematurePrefix,
                VerifierFault::MissingClientCleanup,
                VerifierFault::FailedClientCleanup,
                VerifierFault::MissingBodyError,
                VerifierFault::FalseBodyEof,
                VerifierFault::MissingClientSubmission,
            ] {
                failed_body
                    .verify_rejects(fault)
                    .expect("unrelated body-error outcome must be rejected");
            }
        }
    }

    #[test]
    fn startup_failure_still_joins() {
        for protocol in [Protocol::Http1, Protocol::Http2] {
            let mut report = qualified(protocol, Scenario::StartupFailure);
            assert!(report.observations.closed_worker_before_startup);
            report
                .verify_rejects(VerifierFault::WrongRuntimeStartup)
                .expect("wrong-runtime startup must be rejected");
            report
                .verify_rejects(VerifierFault::ProjectionRun)
                .expect("unrelated failed-startup Run error must be rejected");
            assert_eq!(report.observations.dial_attempts, 0);
            assert_eq!(report.observations.bodies_created, 0);
            assert!(!report.has_retained_owners());
        }
    }

    #[test]
    fn cancellation_and_shutdown_keep_reactor_alive() {
        for protocol in [Protocol::Http1, Protocol::Http2] {
            let mut report = qualified(protocol, Scenario::Cancellation);
            assert!(report.observations.explicit_cancellation);
            assert_eq!(report.observations.download.bytes(), b"public-");
            assert_eq!(report.observations.bodies_created, 2);
            assert_eq!(report.observations.bodies_dropped, 2);
            assert!(!report.observations.wrong_body_thread);
            assert!(!report.observations.body_after_runtime_drop);
            assert!(report.observations.bodies_destroyed_before_runtime_drop);
            assert_eq!(report.progress.nonempty_prefix_frames, b"public-".len());
            assert!(report.progress.upload_gate_held_through_retirement);
            assert!(!report.progress.upload.gate_opened);
            assert!(!report.progress.upload.gate_sender_lost);
            for fault in [
                VerifierFault::ReadTimeoutBody,
                VerifierFault::ProtocolErrorBody,
                VerifierFault::ProjectionRun,
                VerifierFault::ReadTimeoutRun,
                VerifierFault::ProtocolErrorRun,
                VerifierFault::UploadGateLost,
                VerifierFault::PrematurePrefix,
            ] {
                report
                    .verify_rejects(fault)
                    .expect("unrelated cancellation outcome must be rejected");
            }
            assert!(matches!(&report.phases.reactor_check, Some(Ok(()))));
            assert!(matches!(&report.phases.native_quiescence, Some(Ok(()))));
            assert_eq!(
                report.events.last(),
                Some(&Event::ApplicationRuntimeDropped)
            );
            assert_eq!(
                report
                    .phases
                    .native_join
                    .as_ref()
                    .expect("native join outcome")
                    .as_ref()
                    .expect("physical native join")
                    .workers_joined,
                1,
            );
        }
    }

    fn assert_rejects_substitution<Observation>(
        report: &mut Report,
        field: fn(&mut Report) -> &mut Observation,
        replacement: Observation,
        description: &str,
    ) {
        report.verify().expect("real baseline must qualify");
        let original = std::mem::replace(field(report), replacement);
        let rejected = report.verify().is_err();
        *field(report) = original;
        report.verify().expect("restored real report must qualify");
        assert!(rejected, "verifier accepted {description}");
    }

    fn assert_rejects_server_errors(report: &mut Report, donor: &Report) {
        donor.verify().expect("real donor must qualify");
        let Some(Ok(Err(run_error))) = &donor.phases.server_run else {
            panic!("donor must retain its actual failed ServerRun");
        };
        assert_rejects_substitution(
            report,
            |report| &mut report.phases.server_run,
            Some(Ok(Err(run_error.clone()))),
            "another real report's ServerRun error",
        );
        let Some(Err(exchange_error)) = &donor.phases.server_exchange else {
            panic!("donor must retain its actual failed server exchange");
        };
        assert_rejects_substitution(
            report,
            |report| &mut report.phases.server_exchange,
            Some(Err(exchange_error.clone())),
            "another real report's server exchange error",
        );
    }

    #[test]
    fn h2_cancellation_retains_server_operation_failure() {
        let mut report = qualified(Protocol::Http2, Scenario::Cancellation);
        assert!(matches!(&report.phases.client_run, Some(Ok(Ok(())))));
        let (Some(Err(exchange_error)), Some(Ok(Err(run_error)))) =
            (&report.phases.server_exchange, &report.phases.server_run)
        else {
            panic!("actual server exchange and ServerRun must retain cancellation errors");
        };
        assert!(exchange_error.cleanup_confirmed());
        assert!(run_error.cleanup_confirmed());
        assert!(exchange_error.first_panic().is_none());
        assert!(run_error.first_panic().is_none());
        assert!(std::ptr::eq(
            exchange_error.as_ref().cause(),
            run_error.as_ref().cause(),
        ));
        assert!(report.phases.timeout_progress.is_none());
        assert!(report.observations.explicit_cancellation);
        assert_eq!(report.observations.upload.bytes(), b"public-");
        assert_eq!(report.observations.download.bytes(), b"public-");
        assert!(report.observations.response_error.is_some());
        assert!(!report.observations.download.eof);
        assert_eq!(report.observations.bodies_created, 2);
        assert_eq!(report.observations.bodies_dropped, 2);
        assert!(!report.observations.wrong_body_thread);
        assert!(!report.observations.body_after_runtime_drop);
        assert!(report.observations.bodies_destroyed_before_runtime_drop);
        assert_eq!(report.progress.prefix_bytes_received, b"public-".len());
        assert_eq!(report.progress.nonempty_prefix_frames, b"public-".len());
        assert!(report.progress.client_body_frame_waits >= 8);
        assert!(report.progress.client_cancel_requested);
        assert!(report.progress.server_cancel_requested);
        assert!(report.progress.upload.retired);
        assert!(report.progress.upload_gate_held_through_retirement);
        assert!(!report.progress.upload.gate_opened);
        assert!(!report.progress.upload.gate_sender_lost);
        assert!(report.phases.client_submission_retained_until_shutdown);
        assert!(report.phases.server_reaped_before_client_shutdown);
        assert_eq!(report.observations.dial_attempts, 1);
        assert!(matches!(&report.phases.reactor_check, Some(Ok(()))));
        assert!(matches!(&report.phases.native_quiescence, Some(Ok(()))));
        assert!(!report.has_retained_owners());
        assert_eq!(
            report.events.last(),
            Some(&Event::ApplicationRuntimeDropped)
        );
        let native_join = report
            .phases
            .native_join
            .as_ref()
            .expect("native join outcome")
            .as_ref()
            .expect("physical native join");
        assert_eq!(native_join.workers_joined, 1);
        assert_eq!(native_join.workers_forced, 0);

        for fault in [
            VerifierFault::ReadTimeoutBody,
            VerifierFault::ProtocolErrorBody,
            VerifierFault::ProjectionRun,
            VerifierFault::ReadTimeoutRun,
            VerifierFault::ProtocolErrorRun,
            VerifierFault::UploadGateLost,
            VerifierFault::PrematurePrefix,
            VerifierFault::MissingClientSubmission,
            VerifierFault::UnconfirmedServerExchangeCleanup,
            VerifierFault::UnconfirmedServerRunCleanup,
            VerifierFault::ServerExchangePanic,
            VerifierFault::ServerRunPanic,
        ] {
            report
                .verify_rejects(fault)
                .expect("unrelated H2 cancellation outcome must be rejected");
            report.verify().expect("real report must remain unchanged");
        }
        assert_rejects_substitution(
            &mut report,
            |report| &mut report.phases.client_exchange,
            None,
            "missing client exchange receipt",
        );
        assert_rejects_substitution(
            &mut report,
            |report| &mut report.phases.server_exchange,
            None,
            "missing server exchange receipt",
        );
        assert_rejects_substitution(
            &mut report,
            |report| &mut report.phases.client_run,
            None,
            "missing ClientRun result",
        );
        assert_rejects_substitution(
            &mut report,
            |report| &mut report.phases.server_run,
            None,
            "missing ServerRun result",
        );
        assert_rejects_substitution(
            &mut report,
            |report| &mut report.phases.server_run,
            Some(Ok(Ok(()))),
            "healthy substituted ServerRun",
        );
        assert_rejects_substitution(
            &mut report,
            |report| &mut report.phases.server_exchange,
            Some(Ok(())),
            "healthy substituted server exchange",
        );
        for missing in [Event::ClientRunJoined, Event::ServerRunJoined] {
            let events = report
                .events
                .iter()
                .copied()
                .filter(|event| *event != missing)
                .collect();
            assert_rejects_substitution(
                &mut report,
                |report| &mut report.events,
                events,
                "missing independent Run parent join event",
            );
        }
        assert_rejects_substitution(
            &mut report,
            |report| &mut report.phases.native_quiescence,
            None,
            "missing native quiescence receipt",
        );
        assert_rejects_substitution(
            &mut report,
            |report| &mut report.phases.reactor_check,
            None,
            "missing reactor check receipt",
        );
        assert_rejects_substitution(
            &mut report,
            |report| &mut report.phases.native_join,
            None,
            "missing physical native join receipt",
        );
        assert_rejects_substitution(
            &mut report,
            |report| {
                &mut report
                    .phases
                    .native_join
                    .as_mut()
                    .expect("actual native join receipt")
                    .as_mut()
                    .expect("successful native join")
                    .workers_forced
            },
            1,
            "forced native worker",
        );

        let cancelled_donor = qualified(Protocol::Http2, Scenario::Cancellation);
        let (Some(Err(exchange_error)), Some(Ok(Err(donor_error)))) = (
            &report.phases.server_exchange,
            &cancelled_donor.phases.server_run,
        ) else {
            panic!("independent cancellation reports must retain their actual server errors");
        };
        assert!(
            !std::ptr::eq(
                exchange_error.as_ref().cause(),
                donor_error.as_ref().cause(),
            ),
            "independent cancellations must expose distinct retained causes",
        );
        assert_rejects_server_errors(&mut report, &cancelled_donor);

        let body_error_donor = qualified(Protocol::Http2, Scenario::BodyError);
        assert_rejects_server_errors(&mut report, &body_error_donor);
    }
}
