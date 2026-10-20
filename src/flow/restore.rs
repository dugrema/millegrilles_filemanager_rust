use crate::flow::app_service::ApplicationService;
use millegrilles_common_rust::error::Error as CommonError;
use millegrilles_common_rust::openssl::pkey::{PKey, Private};
use millegrilles_common_rust::tokio::time::sleep;
use millegrilles_common_rust::tokio_util::sync::CancellationToken;
use millegrilles_common_rust::tracing::{error, info};
use std::sync::Arc;

pub async fn restore_from_backup(
    app_service: Arc<ApplicationService>,
    no_resume: bool,
    master_key: &PKey<Private>,
    shutdown_token: CancellationToken
) {
    let return_code = match restore(app_service.as_ref(), master_key, ! no_resume).await {
        Ok(()) => {
            info!("Restoration process complete - shutting down");
            0
        },
        Err(e) => {
            error!("Error during restoration: {:?}", e);
            shutdown_token.cancel();
            2
        }
    };

    // Stop all processes - restoration complete
    shutdown_token.cancel();

    sleep(std::time::Duration::from_secs(2)).await;
    std::process::exit(return_code);
}

async fn restore(
    app_service: &ApplicationService,
    master_key: &PKey<Private>,
    resume: bool
) -> Result<(), CommonError> {
    info!("Beginning database restoration");
    let result = app_service.restore(
        Some(&master_key),
        resume,
        None,
    ).await?;

    // Produce final restoration report
    info!(
        "Transactions processed {} transactions ({} skipped then {} resumed)",
        result.transaction_count,
        result.initially_skipped,
        result.transaction_count - result.initially_skipped,
    );

    Ok(())
}
