#![feature(lazy_cell)]

use std::{future::Future, sync::LazyLock};

use orkas_core::{Handle as OrkasHandle, Orkas, OrkasConfig};
use rustler::{
    nif,
    types::atom::{error, ok},
    Encoder, Env, Error, NifResult, OwnedEnv, ResourceArc, Term,
};
use tap::Pipe;
use tokio::{
    runtime::{Builder, Runtime},
    task::JoinHandle,
};

static RT: LazyLock<Runtime> = LazyLock::new(|| {
    Builder::new_current_thread()
        .enable_all()
        .thread_name("orkas-worker")
        .build()
        .expect("Failed to build Tokio runtime")
});

pub mod atom {
    use rustler::atoms;

    atoms! {
        topic_join_succeeded,
        topic_join_failed,
    }
}

fn load(env: Env, _info: Term) -> bool {
    rustler::resource!(OrkasResource, env);
    rustler::resource!(HandleResource, env);

    true
}

struct OrkasResource {
    orkas: Orkas,
}

#[nif(schedule = "DirtyIo")]
fn init() -> NifResult<ResourceArc<OrkasResource>> {
    RT.block_on(async {
        OrkasConfig::simple("127.0.0.1:3000".parse().unwrap())
            .start()
            .await
    })
    .or_else(|e| Error::Term(format!("Orkas initiation failed: {e}").pipe(Box::new)).pipe(Err))?
    .pipe(|orkas| OrkasResource { orkas })
    .pipe(ResourceArc::new)
    .pipe(Ok)
}

struct HandleResource {
    orkas: OrkasHandle,
}

impl HandleResource {
    fn run<F, R, Fut>(&self, f: F) -> JoinHandle<R>
    where
        F: FnOnce(OrkasHandle, OwnedEnv) -> Fut,
        Fut: Future<Output = R> + Send + 'static,
        R: Send + 'static,
    {
        f(self.orkas.clone(), OwnedEnv::new()).pipe(|fut| RT.spawn(fut))
    }
}

#[nif]
fn create_handle(rt: ResourceArc<OrkasResource>) -> ResourceArc<HandleResource> {
    let orkas = rt.orkas.handle();

    HandleResource { orkas }.pipe(ResourceArc::new)
}

#[nif]
fn join(env: Env<'_>, handle: ResourceArc<HandleResource>, topic: String, address: String) {
    let pid = env.pid();
    let handle: &HandleResource = &handle;

    handle.run(|orkas, mut env| async move {
        match orkas
            .join_one(topic.clone(), address.parse().unwrap())
            .await
        {
            Ok(_) => env.send_and_clear(&pid, |e| {
                (ok(), atom::topic_join_succeeded(), topic, address).encode(e)
            }),
            Err(err) => {
                env.send_and_clear(&pid, |e| {
                    (
                        error(),
                        atom::topic_join_failed(),
                        topic,
                        address,
                        err.to_string(),
                    )
                        .encode(e)
                });
            }
        }
    });
}

rustler::init!("Elixir.Orkas", [init, create_handle], load = load);
