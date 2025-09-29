use anyhow::{Result, anyhow};
use std::collections::hash_map::Entry;
use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, Instant};

use crate::protocol::state::ProtocolState;
use crate::protocol::{MsgId, OpCode, ProtocolStateful, RingMessage};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Actor {
    Sender,
    Receiver,
}

const MAILBOX_EXPIRY: Duration = Duration::from_secs(30);

// TODO: context between worker and mailbox is that Worker is able to handle all
// terminal states. It is the responsibility of the worker to execute these terminal actions.
pub struct Mailbox {
    mail: HashMap<MsgId, ProtocolStateful>,
    expired: BTreeSet<(Instant, MsgId)>,
}

impl Mailbox {
    pub fn new() -> Self {
        Self {
            mail: HashMap::new(),
            expired: BTreeSet::new(),
        }
    }

    pub fn insert_new(&mut self, state: ProtocolStateful) -> Result<()> {
        let msg_id = state.msg_id();

        match self.mail.entry(msg_id) {
            Entry::Occupied(entry) => {
                return Err(anyhow!(
                    "Message ID collision in Mailbox: {:?}",
                    *entry.key()
                ));
            }
            Entry::Vacant(entry) => {
                entry.insert(state);
            }
        }

        self.expired
            .insert((Instant::now() + MAILBOX_EXPIRY, msg_id));

        Ok(())
    }

    // Returns Some(state) if the state was terminal and we need to handle the terminal action
    // associated with the terminal state.
    pub fn receive(&mut self, msg: RingMessage) -> Result<Option<ProtocolStateful>> {
        if self.mail.contains_key(&msg.msg_id()) {
            self.receive_next(msg)
        } else {
            self.receive_new(msg)
        }
    }

    fn receive_new(&mut self, msg: RingMessage) -> Result<Option<ProtocolStateful>> {
        let state = ProtocolStateful::try_new(msg, Actor::Receiver)?;

        if state.is_terminal() {
            Ok(Some(state))
        } else {
            self.insert_new(state)?;
            Ok(None)
        }
    }

    // TODO: check if msg was expired?
    fn receive_next(&mut self, msg: RingMessage) -> Result<Option<ProtocolStateful>> {
        let msg_id = msg.msg_id();
        if !self.mail.contains_key(&msg_id) {
            return Err(anyhow!(
                "Received message for unknown message ID: {:?}",
                msg_id
            ));
        }

        let new_state = self.mail.get_mut(&msg_id).unwrap().step(msg)?;

        if !new_state.is_terminal() {
            Ok(None)
        } else {
            // Safety: we checked existence above
            self.mail.remove(&new_state.msg_id()).unwrap();
            Ok(Some(new_state))
        }
    }

    pub fn send(&mut self, msg: RingMessage) -> Result<Option<ProtocolStateful>> {
        // TODO: interface does not exist
        // let sqe = SqeRingMessage::from(&msg);

        // TODO: add SqeRingMessage to futures (task)

        if msg.opcode() == OpCode::Ack {
            return Ok(None);
        }

        let state = ProtocolStateful::try_new(msg, Actor::Sender)?;
        if state.is_terminal() {
            Ok(Some(state))
        } else {
            self.insert_new(state)?;
            Ok(None)
        }
    }

    // TODO:
    // - periodically call as part of maintenance task
    // - worker needs to handle the expired `Vec<ProtocolStateful>`, should all be non-terminal
    // states
    pub fn remove_expired(&mut self) -> Vec<ProtocolStateful> {
        let now = Instant::now();
        let mut expired = Vec::new();

        loop {
            let msg_id = match self.expired.first() {
                Some((timestamp, msg_id)) => {
                    if *timestamp >= now {
                        break;
                    }
                    *msg_id
                }
                None => break,
            };

            self.expired.pop_first();

            // Ignore completed messages
            if let Some(state) = self.mail.remove(&msg_id) {
                expired.push(state);
            }
        }

        expired
    }
}
