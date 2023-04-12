use std::{sync::Arc, thread};

use orkas_core::{Handle as OrkasHandle, Orkas, OrkasConfig};
use rustler::{nif, Env, Error, NifResult, ResourceArc, Term};
use tap::Pipe;
use tokio::{runtime::Handle as TokioHandle, sync::Notify};

fn load(env: Env, _info: Term) -> bool {
    rustler::resource!(RuntimeResource, env);
    rustler::resource!(HandleResource, env);

    true
}

struct RuntimeResource {
    orkas: Orkas,
    rt: TokioHandle,
    notify: Arc<Notify>,
}

#[nif(schedule = "DirtyIo")]
fn init() -> NifResult<ResourceArc<RuntimeResource>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("orkas-worker")
        .build()
        .or_else(|e| Error::Term(e.to_string().pipe(Box::new)).pipe(Err))?;
    let rt = runtime.handle().clone();
    let notify = Notify::new().pipe(Arc::new);

    let orkas = runtime
        .block_on(async {
            OrkasConfig::simple("127.0.0.1:3000".parse().unwrap())
                .start()
                .await
        })
        .or_else(|e| {
            Error::Term(format!("Orkas initiation failed: {e}").pipe(Box::new)).pipe(Err)
        })?;

    notify
        .clone()
        .pipe(|n| move || runtime.block_on(n.notified()))
        .pipe(thread::spawn);

    Ok(ResourceArc::new(RuntimeResource { orkas, rt, notify }))
}

impl Drop for RuntimeResource {
    fn drop(&mut self) {
        self.rt.block_on(self.orkas.stop());
        self.notify.notify_waiters();
    }
}

struct HandleResource {
    rt: TokioHandle,
    orkas: OrkasHandle,
}

#[nif]
fn create_handle(rt: ResourceArc<RuntimeResource>) -> ResourceArc<HandleResource> {
    let orkas = rt.orkas.handle();
    let rt = rt.rt.clone();

    HandleResource { rt, orkas }.pipe(ResourceArc::new)
}

rustler::init!("Elixir.Orkas", [init, create_handle], load = load);
