use agentdp_guest::cli_run;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    match cli_run().await {
        Ok(()) => {}
        Err(error) => {
            eprintln!("guestctl: {error}");
            std::process::exit(1);
        }
    }
}
