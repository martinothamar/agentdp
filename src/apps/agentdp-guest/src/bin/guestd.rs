use agentdp_guest::daemon_run;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(error) = daemon_run().await {
        eprintln!("guestd: {error}");
        std::process::exit(1);
    }
}
