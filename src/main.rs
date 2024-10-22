use std::env;
use std::time::Duration;

use anyhow::Result;
use futures_util::future::{BoxFuture, Shared};
use futures_util::FutureExt;
use tokio::signal;
use tracing::instrument;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};

pub type SomeFuture = Shared<BoxFuture<'static, Option<()>>>;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::WARN.into())
                .from_env_lossy(),
        )
        .init();

    let (shutdown_tx, shutdown_rx) = flume::bounded(0);

    // close channel to trigger bug
    let future: Shared<BoxFuture<Option<()>>> = async {
        for x in (1..=3).rev() {
            tracing::warn!("running in {x}s..");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        tracing::warn!("🚀");
        Some(())
    }
    .boxed()
    .shared();

    let arg1 = env::args().nth(1).unwrap_or("bug".to_string());
    let handle = match arg1.as_str() {
        "bug" => tokio::spawn(bug(shutdown_rx, future)),
        "fix1" => tokio::spawn(fix1(shutdown_rx, future)),
        "fix2" => tokio::spawn(fix2(shutdown_rx, future)),
        "fix3" => tokio::spawn(fix3(shutdown_rx, future)),
        "fix4" => tokio::spawn(fix4(shutdown_rx, future)),
        "fix5" => tokio::spawn(fix5(shutdown_rx, future)),
        _ => panic!("unhandled arg: {arg1}"),
    };

    // let run for 10 second
    tokio::select! {
        _ = signal::ctrl_c() => {}
        _ = tokio::time::sleep(Duration::from_secs(10)) => {}
    }

    tracing::error!("shutting down..");
    drop(shutdown_tx);

    // wait for spawned task
    tokio::select! {
        _ = signal::ctrl_c() => {}
        _ = handle => {}
        _ = tokio::time::sleep(Duration::from_secs(10)) => panic!("spawned task did not finish"),
    }

    Ok(())
}

// problematic code that made worker use 100% cpu. The root cause was because the `tokio::select!`
// would always trigger the rx.recv_async() arm once the channel had been dropped. A dropped
// channel will return errors when we read from it.
#[instrument(skip_all)]
async fn bug(shutdown: flume::Receiver<()>, fut: SomeFuture) {
    let (tx, rx) = flume::bounded(1);

    // propagate future via channel
    {
        let fut = fut.clone();
        tokio::spawn(async move {
            let _ = fut.await;
            let _ = tx.send(());
            // drop(tx); // <- here tx will be dropped, closing the channel
        });
    }

    loop {
        tokio::select! {
            // 100% cpu usage -- tx.recv_async() will always trigger since the channel is closed.
            _ = rx.recv_async() => tracing::trace!("recv from future!"),
            _ = shutdown.recv_async() => break,
        }
    }

    _ = fut.await;

    tracing::info!("exit");
}

// very simple fix is to add Ok() -- this actually fixes the problem.
#[instrument(skip_all)]
async fn fix1(shutdown: flume::Receiver<()>, fut: SomeFuture) {
    let (tx, rx) = flume::bounded(1);

    // propagate future via channel
    {
        let fut = fut.clone();
        tokio::spawn(async move {
            let _ = fut.await;
            let _ = tx.send(());
        });
    }

    loop {
        tokio::select! {
            // only trigger on Ok
            Ok(_) = rx.recv_async() => tracing::info!("recv!"),
            _ = shutdown.recv_async() => break,
        }
    }

    _ = fut.await;

    tracing::info!("exit");
}

// an alternative fix is to add an if to the recv_async() arm.
#[instrument(skip_all)]
async fn fix2(shutdown: flume::Receiver<()>, fut: SomeFuture) {
    let (tx, rx) = flume::bounded(1);

    // propagate future via channel
    let mut state = false;
    {
        let fut = fut.clone();
        tokio::spawn(async move {
            let _ = fut.await;
            let _ = tx.send(());
        });
    }
    loop {
        tokio::select! {
            // alternative solution to Ok -- if disables the select arm
            _ = rx.recv_async(), if !state => {
                state = true;
                tracing::info!("recv!");
            }
            _ = shutdown.recv_async() => break,
        }
    }

    _ = fut.await;

    tracing::info!("exit");
}

// in all previous versions, we spawned a separate task only to consume a future. Can we consume
// the future directly?
#[instrument(skip_all)]
async fn fix3(shutdown: flume::Receiver<()>, fut: SomeFuture) {
    let mut state = false;

    // extra channel is removed, use future directly
    loop {
        tokio::select! {
            // clone the shared future
            Some(_) = fut.clone(), if !state => {
                state = true;
                tracing::info!("future!");
            }
            _ = shutdown.recv_async() => break,
        }
    }

    _ = fut.await;

    tracing::info!("exit");
}

// previous version worked fine, but does a clone() every iteration. Here we make a try to consume
// the future. The loop worked fine, but we fail when trying to consume the future after the loop.
#[instrument(skip_all)]
async fn fix4(shutdown: flume::Receiver<()>, mut fut: SomeFuture) {
    let mut state: Option<bool> = None;
    loop {
        tokio::select! {
            // own the future and mutate it
            value = &mut fut, if state.is_none() => {
                state = Some(value.is_some());
                tracing::info!("future!");
            }
            _ = shutdown.recv_async() => break,
        }
    }

    _ = fut.await; // FAIL: future already consumed. Compiler doesn't warn us. :(

    tracing::info!("exit");
}

// in this version we create a fused future, that only triggers once, making us able to remove the
// `, if [..]` in the `tokio::select!` arm. We also clone the future to be able to consume it after
// the loop too.
#[instrument(skip_all)]
async fn fix5(shutdown: flume::Receiver<()>, fut: SomeFuture) {
    let mut state: Option<bool> = None;
    let mut fut_loop = fut.clone().fuse();
    loop {
        tokio::select! {
            // use a fused future
            value = &mut fut_loop => { // the if state.is_none() is not required
                assert!(state.is_none());
                state = Some(value.is_some());
                tracing::info!("future!");
            }
            _ = shutdown.recv_async() => break,
        }
    }

    _ = fut.await;

    tracing::info!("exit");
}
