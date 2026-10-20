//! A small, transport-agnostic SWIM membership implementation.
//!
//! [`Swim`] owns a live member table, probes peers with ping/ack messages, marks
//! unresponsive peers suspect, and eventually marks them dead. Membership
//! changes are published through a tokio `watch` channel and exposed through the
//! [`shardtable::Membership`] trait.
//!
//! Networking is deliberately abstracted behind [`Transport`]. Callers drive the
//! state machine with [`Swim::tick`] and [`Swim::handle`], then deliver the
//! returned [`Outbound`] messages through their transport.

#![warn(missing_docs)]

use std::collections::BTreeMap;
use std::error::Error;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, Instant};

use shardtable::{MemberChanges, Membership};
use tokio::sync::watch;
use uuid::Uuid;

/// Unique identifier of a SWIM member.
pub type MemberId = Uuid;

/// SWIM liveness state for a member.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// The member is responding to probes.
    Alive,
    /// The member missed a probe and may be failing.
    Suspect,
    /// The member is no longer considered part of the cluster.
    Dead,
}

impl Status {
    fn precedence(self) -> u8 {
        match self {
            Self::Alive => 0,
            Self::Suspect => 1,
            Self::Dead => 2,
        }
    }
}

/// A member record exchanged by SWIM gossip.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    /// Stable member identity.
    pub id: MemberId,
    /// Address used to send SWIM messages.
    pub addr: SocketAddr,
    /// Incarnation used to refute stale suspicion.
    pub incarnation: u64,
    /// Current liveness state.
    pub status: Status,
}

impl Member {
    /// Creates an alive member record.
    pub fn alive(id: MemberId, addr: SocketAddr, incarnation: u64) -> Self {
        Self {
            id,
            addr,
            incarnation,
            status: Status::Alive,
        }
    }
}

/// Messages exchanged between SWIM members.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// A direct liveness probe.
    Ping {
        /// Sender record.
        from: Member,
    },
    /// Response to a direct probe.
    Ack {
        /// Sender record.
        from: Member,
    },
    /// Membership state exchange.
    Gossip {
        /// Sender record.
        from: Member,
        /// Sender's current member table.
        members: Vec<Member>,
    },
}

/// A message that should be sent to a peer address.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outbound {
    /// Destination address.
    pub to: SocketAddr,
    /// Message to send.
    pub message: Message,
}

/// Abstraction over the network used by the SWIM state machine.
#[allow(async_fn_in_trait)]
pub trait Transport {
    /// Error returned while sending a message.
    type Error: Error + Send + Sync + 'static;

    /// Sends one SWIM message.
    async fn send(&self, to: SocketAddr, message: Message) -> Result<(), Self::Error>;
}

/// Sends all pending SWIM messages through `transport`.
pub async fn send_all<T, I>(transport: &T, outbounds: I) -> Result<(), T::Error>
where
    T: Transport,
    I: IntoIterator<Item = Outbound>,
{
    for outbound in outbounds {
        transport.send(outbound.to, outbound.message).await?;
    }
    Ok(())
}

/// Timing configuration for failure detection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// Delay between direct probes.
    pub ping_interval: Duration,
    /// Time allowed for a direct ping acknowledgement.
    pub ping_timeout: Duration,
    /// Time a suspect member is given to refute suspicion before becoming dead.
    pub suspect_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            ping_interval: Duration::from_secs(1),
            ping_timeout: Duration::from_millis(500),
            suspect_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct PendingProbe {
    target: MemberId,
    deadline: Instant,
}

struct State {
    local_id: MemberId,
    members: BTreeMap<MemberId, Member>,
    changes: watch::Sender<Vec<MemberId>>,
    config: Config,
    pending: Option<PendingProbe>,
    next_probe: Instant,
    suspect_deadlines: BTreeMap<MemberId, Instant>,
    cursor: usize,
}

/// A cloneable SWIM membership state machine.
#[derive(Clone)]
pub struct Swim {
    state: Arc<RwLock<State>>,
}

impl Swim {
    /// Creates a SWIM node with the default failure-detection timing.
    pub fn new(local_id: MemberId, local_addr: SocketAddr) -> Self {
        Self::with_config(local_id, local_addr, Config::default())
    }

    /// Creates a SWIM node with explicit timing configuration.
    pub fn with_config(local_id: MemberId, local_addr: SocketAddr, config: Config) -> Self {
        let local = Member::alive(local_id, local_addr, 1);
        let (changes, _) = watch::channel(vec![local_id]);
        Self {
            state: Arc::new(RwLock::new(State {
                local_id,
                members: BTreeMap::from([(local_id, local)]),
                changes,
                config,
                pending: None,
                next_probe: Instant::now(),
                suspect_deadlines: BTreeMap::new(),
                cursor: 0,
            })),
        }
    }

    /// Returns the local member identifier.
    pub fn local_id(&self) -> MemberId {
        self.read().local_id
    }

    /// Returns the local member record.
    pub fn local_member(&self) -> Member {
        self.read().local_member()
    }

    /// Returns the configured timing values.
    pub fn config(&self) -> Config {
        self.read().config
    }

    /// Adds a seed member and returns a ping that can be sent to it.
    ///
    /// The seed's gossip response introduces the rest of its member table.
    pub fn join(&self, seed_id: MemberId, seed_addr: SocketAddr) -> Outbound {
        let mut state = self.write();
        state.observe_alive(Member::alive(seed_id, seed_addr, 0));
        state.notify();
        let from = state.local_member();
        Outbound {
            to: seed_addr,
            message: Message::Ping { from },
        }
    }

    /// Handles an incoming SWIM message and returns messages to send.
    pub fn handle(&self, message: Message) -> Vec<Outbound> {
        self.handle_at(message, Instant::now())
    }

    fn handle_at(&self, message: Message, now: Instant) -> Vec<Outbound> {
        let mut state = self.write();
        let mut outbounds = Vec::new();
        let mut changed;
        let gossip_sender;

        match message {
            Message::Ping { from } => {
                changed = state.observe_alive(from.clone());
                let local = state.local_member();
                gossip_sender = Some(from.id);
                outbounds.push(Outbound {
                    to: from.addr,
                    message: Message::Ack {
                        from: local.clone(),
                    },
                });
                outbounds.push(Outbound {
                    to: from.addr,
                    message: Message::Gossip {
                        from: local,
                        members: state.gossip(),
                    },
                });
            }
            Message::Ack { from } => {
                gossip_sender = Some(from.id);
                changed = state.observe_alive(from.clone());
                if state
                    .pending
                    .is_some_and(|pending| pending.target == from.id)
                {
                    state.pending = None;
                }
            }
            Message::Gossip { from, members } => {
                gossip_sender = Some(from.id);
                changed = state.apply_update(from, now);
                for member in members {
                    changed |= state.apply_update(member, now);
                }
            }
        }

        if changed {
            outbounds.extend(state.gossip_outbounds(gossip_sender));
            state.notify();
        }
        outbounds
    }

    /// Advances failure detection at `now` and returns messages to send.
    pub fn tick(&self, now: Instant) -> Vec<Outbound> {
        let mut state = self.write();
        let config = state.config;
        let mut changed = false;

        let expired: Vec<MemberId> = state
            .suspect_deadlines
            .iter()
            .filter_map(|(id, deadline)| (*deadline <= now).then_some(*id))
            .collect();
        for id in expired {
            state.suspect_deadlines.remove(&id);
            if let Some(member) = state.members.get_mut(&id)
                && member.status == Status::Suspect
            {
                member.status = Status::Dead;
                changed = true;
            }
        }

        if let Some(pending) = state.pending {
            let still_alive = state
                .members
                .get(&pending.target)
                .is_some_and(|member| member.status == Status::Alive);
            if !still_alive {
                state.pending = None;
            } else if pending.deadline <= now {
                state.pending = None;
                if let Some(member) = state.members.get_mut(&pending.target)
                    && member.status == Status::Alive
                {
                    member.status = Status::Suspect;
                    state
                        .suspect_deadlines
                        .insert(pending.target, now + config.suspect_timeout);
                    changed = true;
                }
            }
        }

        let mut outbounds = Vec::new();
        if changed {
            outbounds.extend(state.gossip_outbounds(None));
            state.notify();
        }

        if state.pending.is_none()
            && now >= state.next_probe
            && let Some(target) = state.next_target()
        {
            let from = state.local_member();
            outbounds.push(Outbound {
                to: target.addr,
                message: Message::Ping { from },
            });
            state.pending = Some(PendingProbe {
                target: target.id,
                deadline: now + config.ping_timeout,
            });
            state.next_probe = now + config.ping_interval;
        }

        outbounds
    }

    /// Returns the current alive member identifiers in stable order.
    pub fn alive_members(&self) -> Vec<MemberId> {
        self.read().alive_members()
    }

    /// Returns a member's current status, if known.
    pub fn status(&self, id: MemberId) -> Option<Status> {
        self.read().members.get(&id).map(|member| member.status)
    }

    /// Returns a snapshot of all known member records.
    pub fn members(&self) -> Vec<Member> {
        self.read().members.values().cloned().collect()
    }

    /// Subscribes to alive-member snapshots.
    pub fn subscribe(&self) -> MemberChanges<MemberId> {
        self.read().changes.subscribe()
    }

    fn read(&self) -> RwLockReadGuard<'_, State> {
        self.state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn write(&self) -> RwLockWriteGuard<'_, State> {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl Membership for Swim {
    type Member = MemberId;

    fn members(&self) -> impl IntoIterator<Item = Self::Member> {
        self.alive_members()
    }

    fn subscribe(&self) -> MemberChanges<Self::Member> {
        Swim::subscribe(self)
    }
}

impl State {
    fn local_member(&self) -> Member {
        self.members
            .get(&self.local_id)
            .cloned()
            .expect("the local member is always present")
    }

    fn alive_members(&self) -> Vec<MemberId> {
        self.members
            .iter()
            .filter_map(|(id, member)| (member.status == Status::Alive).then_some(*id))
            .collect()
    }

    fn gossip(&self) -> Vec<Member> {
        self.members.values().cloned().collect()
    }

    fn gossip_outbounds(&self, except: Option<MemberId>) -> Vec<Outbound> {
        let from = self.local_member();
        let members = self.gossip();
        self.members
            .values()
            .filter(|member| member.status == Status::Alive && member.id != self.local_id)
            .filter(|member| Some(member.id) != except)
            .map(|member| Outbound {
                to: member.addr,
                message: Message::Gossip {
                    from: from.clone(),
                    members: members.clone(),
                },
            })
            .collect()
    }

    fn next_target(&mut self) -> Option<Member> {
        let peers: Vec<Member> = self
            .members
            .values()
            .filter(|member| member.status == Status::Alive && member.id != self.local_id)
            .cloned()
            .collect();
        if peers.is_empty() {
            return None;
        }
        let target = peers[self.cursor % peers.len()].clone();
        self.cursor = (self.cursor + 1) % peers.len();
        Some(target)
    }

    fn observe_alive(&mut self, update: Member) -> bool {
        if update.id == self.local_id {
            return false;
        }

        let incarnation = self
            .members
            .get(&update.id)
            .map_or(update.incarnation, |current| {
                current.incarnation.max(update.incarnation)
            });
        let next = Member::alive(update.id, update.addr, incarnation);
        self.suspect_deadlines.remove(&update.id);
        if self.members.get(&update.id) == Some(&next) {
            return false;
        }
        self.members.insert(update.id, next);
        true
    }

    fn apply_update(&mut self, update: Member, now: Instant) -> bool {
        if update.id == self.local_id {
            let current = self.local_member();
            if update.status != Status::Alive && update.incarnation >= current.incarnation {
                self.members.insert(
                    self.local_id,
                    Member::alive(self.local_id, current.addr, current.incarnation + 1),
                );
                return true;
            }
            return false;
        }

        let next = match self.members.get(&update.id) {
            None => Some(update),
            Some(current) if update.incarnation > current.incarnation => Some(update),
            Some(current)
                if update.incarnation == current.incarnation
                    && update.status.precedence() > current.status.precedence() =>
            {
                Some(Member {
                    status: update.status,
                    ..current.clone()
                })
            }
            Some(_) => None,
        };

        let Some(next) = next else {
            return false;
        };
        match next.status {
            Status::Alive => {
                self.suspect_deadlines.remove(&next.id);
            }
            Status::Suspect => {
                self.suspect_deadlines
                    .insert(next.id, now + self.config.suspect_timeout);
            }
            Status::Dead => {
                self.suspect_deadlines.remove(&next.id);
            }
        }
        self.members.insert(next.id, next);
        true
    }

    fn notify(&self) {
        let members = self.alive_members();
        if self.changes.borrow().as_slice() != members.as_slice() {
            self.changes.send_replace(members);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn rapid_config() -> Config {
        Config {
            ping_interval: Duration::from_secs(1),
            ping_timeout: Duration::from_millis(100),
            suspect_timeout: Duration::from_secs(1),
        }
    }

    #[test]
    fn membership_exposes_alive_members_and_notifications() {
        let local = Uuid::new_v4();
        let seed = Uuid::new_v4();
        let swim = Swim::new(local, addr(1000));
        let mut changes = swim.subscribe();

        swim.join(seed, addr(1001));

        assert!(changes.has_changed().unwrap());
        let mut expected = vec![local, seed];
        expected.sort();
        assert_eq!(changes.borrow_and_update().clone(), expected);
        assert_eq!(
            Membership::members(&swim).into_iter().collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn acknowledgement_keeps_a_member_alive() {
        let local = Uuid::new_v4();
        let peer = Uuid::new_v4();
        let swim = Swim::with_config(local, addr(1100), rapid_config());
        let other = Swim::with_config(peer, addr(1101), rapid_config());
        swim.join(peer, addr(1101));

        let now = Instant::now();
        let ping = swim
            .tick(now)
            .into_iter()
            .find(|outbound| matches!(outbound.message, Message::Ping { .. }))
            .expect("ping");
        let ack = other
            .handle(ping.message)
            .into_iter()
            .find(|outbound| matches!(outbound.message, Message::Ack { .. }))
            .expect("ack");
        swim.handle(ack.message);
        swim.tick(now + Duration::from_secs(2));

        assert_eq!(swim.status(peer), Some(Status::Alive));
    }

    #[test]
    fn missed_ack_marks_member_suspect_then_dead() {
        let local = Uuid::new_v4();
        let peer = Uuid::new_v4();
        let swim = Swim::with_config(local, addr(1200), rapid_config());
        let changes = swim.subscribe();
        swim.join(peer, addr(1201));

        let now = Instant::now();
        swim.tick(now);
        swim.tick(now + Duration::from_millis(200));

        assert_eq!(swim.status(peer), Some(Status::Suspect));
        assert!(changes.has_changed().unwrap());
        assert_eq!(
            Membership::members(&swim).into_iter().collect::<Vec<_>>(),
            vec![local]
        );

        swim.tick(now + Duration::from_secs(2));
        assert_eq!(swim.status(peer), Some(Status::Dead));
    }

    #[test]
    fn suspecting_local_member_increments_its_incarnation() {
        let local = Uuid::new_v4();
        let swim = Swim::new(local, addr(1300));
        let suspect = Member {
            id: local,
            addr: addr(1300),
            incarnation: 1,
            status: Status::Suspect,
        };

        let before = swim.local_member().incarnation;
        swim.handle(Message::Gossip {
            from: suspect.clone(),
            members: vec![suspect],
        });

        assert_eq!(swim.status(local), Some(Status::Alive));
        assert!(swim.local_member().incarnation > before);
    }

    #[test]
    fn coordinator_uses_swim_as_its_membership_source() {
        let local = Uuid::new_v4();
        let seed = Uuid::new_v4();
        let swim = Swim::new(local, addr(1350));
        swim.join(seed, addr(1351));

        let coordinator = shardtable::Coordinator::new(swim, 64);

        assert_eq!(coordinator.table().len(), 2);
    }

    #[derive(Default)]
    struct RecordingTransport {
        sent: std::sync::Mutex<Vec<Outbound>>,
    }

    impl Transport for RecordingTransport {
        type Error = std::io::Error;

        async fn send(&self, to: SocketAddr, message: Message) -> Result<(), Self::Error> {
            self.sent.lock().unwrap().push(Outbound { to, message });
            Ok(())
        }
    }

    #[tokio::test]
    async fn outbound_messages_can_be_sent_through_the_transport_hook() {
        let transport = RecordingTransport::default();
        let message = Outbound {
            to: addr(1400),
            message: Message::Ping {
                from: Member::alive(Uuid::new_v4(), addr(1401), 1),
            },
        };

        send_all(&transport, [message.clone()]).await.unwrap();

        assert_eq!(*transport.sent.lock().unwrap(), vec![message]);
    }
}
