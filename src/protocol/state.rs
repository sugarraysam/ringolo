use std::os::fd::RawFd;

use crate::context::GlobalContext;
use crate::protocol::mailbox::Actor;
use crate::protocol::{MsgId, RingMessage};
use anyhow::{Result, anyhow};
pub trait ProtocolState {
    fn step(&mut self, msg: RingMessage) -> Result<ProtocolStateful>;
}

// Transitions are extremely lightweight. We carry data and advance state machine, but executing
// the final state machine state will be done elsewhere.
#[derive(Copy, Debug, Clone, PartialEq, Eq)]
pub enum ProtocolStateful {
    // Initial states
    WaitingForAck(WaitingForAck),
    // Terminal states
    ShuttingDown(ShuttingDown),
    StealingTask(StealingTask),
    WakingSender(WakingSender),
}

impl ProtocolStateful {
    pub fn try_new(msg: RingMessage, actor: Actor) -> Result<Self> {
        match actor {
            Actor::Sender => Self::try_new_sender(msg),
            Actor::Receiver => Self::try_new_receiver(msg),
        }
    }

    fn try_new_sender(msg: RingMessage) -> Result<Self> {
        match msg {
            RingMessage::Shutdown { msg_id, .. } => {
                Ok(ProtocolStateful::WaitingForAck(WaitingForAck { msg_id }))
            }
            _ => Err(anyhow!(
                "Can't initialized ProtocolStateful from sending side with msg: {:?}",
                msg
            )),
        }
    }

    fn try_new_receiver(msg: RingMessage) -> Result<Self> {
        match msg {
            RingMessage::Shutdown { msg_id, sender_fd } => {
                Ok(ProtocolStateful::ShuttingDown(ShuttingDown {
                    msg_id,
                    sender_fd,
                }))
            }
            RingMessage::StealTask {
                msg_id,
                task_address,
            } => Ok(ProtocolStateful::StealingTask(StealingTask {
                msg_id,
                sender_fd: GlobalContext::instance().get_ring_fd(msg_id.thread_id())?,
                task_address,
            })),
            _ => Err(anyhow!(
                "Can't initialized ProtocolStateful from receiving side with msg: {:?}",
                msg
            )),
        }
    }

    pub fn msg_id(&self) -> MsgId {
        match self {
            ProtocolStateful::WaitingForAck(s) => s.msg_id,
            ProtocolStateful::ShuttingDown(s) => s.msg_id,
            ProtocolStateful::StealingTask(s) => s.msg_id,
            ProtocolStateful::WakingSender(s) => s.msg_id,
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            ProtocolStateful::ShuttingDown(_)
                | ProtocolStateful::StealingTask(_)
                | ProtocolStateful::WakingSender(_)
        )
    }
}

impl ProtocolState for ProtocolStateful {
    fn step(&mut self, msg: RingMessage) -> Result<ProtocolStateful> {
        if self.msg_id() != msg.msg_id() {
            return Err(anyhow!(
                "Message ID mismatch: expected {:?}, got {:?}",
                self.msg_id(),
                msg.msg_id()
            ));
        }

        let next_state = match self {
            ProtocolStateful::WaitingForAck(s) => s.step(msg),
            _ => Err(anyhow!("Cannot step on a terminal state")),
        }?;

        let _old_state = std::mem::replace(self, next_state);
        Ok(next_state)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct WaitingForAck {
    pub msg_id: MsgId,
}

impl ProtocolState for WaitingForAck {
    fn step(&mut self, msg: RingMessage) -> Result<ProtocolStateful> {
        match msg {
            RingMessage::Ack { .. } => Ok(ProtocolStateful::WakingSender(WakingSender {
                msg_id: msg.msg_id(),
            })),
            _ => Err(anyhow!(
                "Invalid state transition: expected Ack, got {:?}",
                msg
            )),
        }
    }
}

// Terminal states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShuttingDown {
    pub msg_id: MsgId,
    pub sender_fd: RawFd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StealingTask {
    pub msg_id: MsgId,
    pub sender_fd: RawFd,
    pub task_address: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakingSender {
    pub msg_id: MsgId,
}
