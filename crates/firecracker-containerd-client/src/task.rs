use std::fmt::Debug;

use containerd_client::{services::v1::CreateTaskResponse, types::Mount};

use crate::{client::Client, error::Result};

#[derive(Debug, Clone)]
pub struct Task {
    container_id: String,
    _exec_id: Option<String>,
    pid: u32,

    exit_status: Option<u32>,
}

impl From<CreateTaskResponse> for Task {
    #[inline]
    fn from(resp: CreateTaskResponse) -> Self {
        Self {
            container_id: resp.container_id,
            _exec_id: None,
            pid: resp.pid,
            exit_status: None,
        }
    }
}

impl Task {
    #[inline]
    pub fn pid(&self) -> u32 {
        self.pid
    }

    #[inline]
    pub(crate) fn set_pid(&mut self, pid: u32) {
        self.pid = pid
    }

    #[inline]
    pub fn container_id(&self) -> &str {
        &self.container_id
    }

    #[inline]
    pub async fn create<S>(client: &Client, container_id: S, rootfs: &[Mount]) -> Result<Self>
    where
        S: Into<String> + Debug,
    {
        client.create_task(container_id, rootfs).await
    }

    #[inline]
    pub async fn start(&mut self, client: &Client) -> Result<()> {
        client.start_task(self).await
    }

    #[inline]
    pub async fn kill(&self, client: &Client, signal_no: u32, all: bool) -> Result<()> {
        client.kill_task(&self.container_id, signal_no, all).await
    }

    #[inline]
    pub async fn delete(&mut self, client: &Client) -> Result<u32> {
        let exit_status = client.delete_task(&self.container_id).await?;
        self.exit_status = Some(exit_status);
        Ok(exit_status)
    }
}
