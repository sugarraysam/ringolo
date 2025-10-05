use crate::context::{ThreadId, with_context_mut};
use anyhow::{Result, anyhow};
use std::convert::TryFrom;
use std::ops::{Deref, DerefMut};
use std::os::fd::RawFd;

// Result field (u32) layout:
// 010101____00000000___000000000000000000
// magic(6)  opcode(8)     msg_id(18)

// 6 first bits (31-26)
// We need to use `u32` for magic mask otherwise we get i32 signed overflow. For
// other mask let's keep i32 because the underlying io_uring crate uses i32 in
// result field, so we want to avoid lot's of casting.
pub const MAGIC: u8 = 0x54;
pub const MAGIC_MASK: u32 = 0xFC00_0000;
pub const MAGIC_BITS: u32 = 6;

// 8 bits (25-18)
pub const OPCODE_MASK: i32 = 0x03FC_0000;
pub const OPCODE_BITS: u32 = 8;

// 18 bits (17-0)
pub const MSG_ID_MASK: i32 = THREAD_ID_MASK | MSG_COUNTER_MASK;
pub const MSG_ID_BITS: u32 = 18;

// 8 bits (17-10)
pub const THREAD_ID_MASK: i32 = 0x0003_FC00;
pub const THREAD_ID_BITS: u32 = 8;

// 10 bits (9-0)
pub const MSG_COUNTER_MASK: i32 = 0x0000_03FF;
pub const MSG_COUNTER_BITS: u32 = 10;
pub const MAX_MSG_COUNTER_VALUE: u32 = 2u32.pow(MSG_COUNTER_BITS);

// We store the message_id on a 32 bit integer, but it is important to remember
// that it will only use 18 bits, 8 for thread_id and 10 for per-thread message
// counter.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, PartialOrd, Ord)]
pub struct MsgId(i32);

impl MsgId {
    pub fn from_result(result: i32) -> Self {
        Self(result & MSG_ID_MASK)
    }
    pub fn thread_id(&self) -> ThreadId {
        ((self.0 & THREAD_ID_MASK) >> MSG_COUNTER_BITS) as u8
    }

    pub fn msg_counter(&self) -> i32 {
        self.0 & MSG_COUNTER_MASK
    }
}

impl Deref for MsgId {
    type Target = i32;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for MsgId {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl From<MsgId> for i32 {
    fn from(val: MsgId) -> Self {
        val.0
    }
}

impl From<i32> for MsgId {
    fn from(msg_id: i32) -> Self {
        MsgId(msg_id)
    }
}

pub fn check_magic(result: i32) -> Result<()> {
    let raw = (result as u32) & MAGIC_MASK;
    let magic = (raw >> (OPCODE_BITS + MSG_ID_BITS)) as u8;
    if magic != MAGIC {
        Err(anyhow!("Invalid magic number in RingMessage."))
    } else {
        Ok(())
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OpCode {
    Ack = 0,
    Shutdown = 1,
    StealTask = 2,
}

impl TryFrom<i32> for OpCode {
    type Error = anyhow::Error;

    fn try_from(result: i32) -> Result<Self, Self::Error> {
        let opcode = ((result & OPCODE_MASK) >> MSG_ID_BITS) as u8;
        match opcode {
            0 => Ok(OpCode::Ack),
            1 => Ok(OpCode::Shutdown),
            2 => Ok(OpCode::StealTask),
            _ => Err(anyhow!("Invalid OpCode value")),
        }
    }
}

impl From<OpCode> for u8 {
    fn from(val: OpCode) -> Self {
        val as u8
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum RingMessage {
    Ack { msg_id: MsgId, sender_fd: RawFd },
    Shutdown { msg_id: MsgId, sender_fd: RawFd },
    StealTask { msg_id: MsgId, task_address: u64 },
}

impl RingMessage {
    pub fn msg_id(&self) -> MsgId {
        match &self {
            RingMessage::Ack { msg_id, .. } => *msg_id,
            RingMessage::Shutdown { msg_id, .. } => *msg_id,
            RingMessage::StealTask { msg_id, .. } => *msg_id,
        }
    }

    pub fn opcode(&self) -> OpCode {
        match &self {
            RingMessage::Ack { .. } => OpCode::Ack,
            RingMessage::Shutdown { .. } => OpCode::Shutdown,
            RingMessage::StealTask { .. } => OpCode::StealTask,
        }
    }

    pub fn new_ack(from: &RingMessage, sender_fd: RawFd) -> Self {
        // Do not generate a new msg_id, ACK uses the msg_id field of the message
        // being ACK'ed so we can match it on the receiving side.
        RingMessage::Ack {
            msg_id: from.msg_id(),
            sender_fd,
        }
    }

    pub fn new_shutdown(sender_fd: RawFd) -> Self {
        RingMessage::Shutdown {
            msg_id: with_context_mut(|ctx| ctx.next_ring_msg_id()),
            sender_fd,
        }
    }

    pub fn new_steal_task(task_address: u64) -> Self {
        RingMessage::StealTask {
            msg_id: with_context_mut(|ctx| ctx.next_ring_msg_id()),
            task_address,
        }
    }

    pub fn try_decode(result: i32, user_data: u64) -> Result<Self> {
        check_magic(result)?;

        let msg_id = MsgId::from_result(result);

        let msg = match OpCode::try_from(result)? {
            OpCode::Ack => RingMessage::Ack {
                msg_id,
                sender_fd: user_data as i32,
            },
            OpCode::Shutdown => RingMessage::Shutdown {
                msg_id,
                sender_fd: user_data as i32,
            },
            OpCode::StealTask => RingMessage::StealTask {
                msg_id,
                task_address: user_data,
            },
        };

        Ok(msg)
    }

    pub fn encode(&self) -> (i32, u64) {
        let magic = (MAGIC as i32) << (OPCODE_BITS + MSG_ID_BITS);

        let (result, user_data) = match &self {
            RingMessage::Ack { msg_id, sender_fd } => {
                let opcode = (OpCode::Ack as i32) << MSG_ID_BITS;
                ((magic | opcode | **msg_id), *sender_fd as u64)
            }
            RingMessage::Shutdown { msg_id, sender_fd } => {
                let opcode = (OpCode::Shutdown as i32) << MSG_ID_BITS;
                ((magic | opcode | **msg_id), *sender_fd as u64)
            }
            RingMessage::StealTask {
                msg_id,
                task_address,
            } => {
                let opcode = (OpCode::StealTask as i32) << MSG_ID_BITS;
                ((magic | opcode | **msg_id), (*task_address))
            }
        };

        (result, user_data)
    }
}
