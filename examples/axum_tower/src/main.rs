use std::process::ExitCode;

use roxygen_axum_tower_fixture::Protocol;
use roxygen_axum_tower_fixture::Scenario;
use roxygen_axum_tower_fixture::run;

fn main() -> ExitCode {
    let mut failed = false;
    for protocol in [Protocol::Http1, Protocol::Http2] {
        for scenario in [
            Scenario::Streaming,
            Scenario::BodyError,
            Scenario::StartupFailure,
            Scenario::Cancellation,
        ] {
            match run(protocol, scenario) {
                Ok(report) => {
                    if let Err(error) = report.verify() {
                        eprintln!("{protocol:?}/{scenario:?}: {error}\n{report:#?}");
                        failed = true;
                    } else {
                        println!("{protocol:?}/{scenario:?}: prototype checks satisfied");
                        println!("  native join: {:?}", report.phases.native_join);
                    }
                }
                Err(error) => {
                    eprintln!("{protocol:?}/{scenario:?}: bootstrap failed: {error}");
                    failed = true;
                }
            }
        }
    }
    if failed {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
