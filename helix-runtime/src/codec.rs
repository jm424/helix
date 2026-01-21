//! Message codec for Raft protocol over TCP.
//!
//! This module provides binary serialization for Raft messages using a
//! simple length-prefixed format suitable for TCP streaming.
//!
//! # Wire Format
//!
//! Each message is framed as:
//! - 4 bytes: message length (u32 little-endian, not including header)
//! - 1 byte: message type tag
//! - N bytes: message-specific payload
//!
//! # Message Types
//!
//! - 0: `PreVote`
//! - 1: `PreVoteResponse`
//! - 2: `RequestVote`
//! - 3: `RequestVoteResponse`
//! - 4: `AppendEntries`
//! - 5: `AppendEntriesResponse`
//! - 6: `TimeoutNow`
//! - 7: `GroupMessageBatch` (for Multi-Raft batched messages)

use bytes::{Buf, BufMut, Bytes, BytesMut};
use helix_core::{GroupId, LogIndex, NodeId, TermId, MAX_RAFT_MESSAGE_BYTES};
use helix_raft::{
    multi::GroupMessage, AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest,
    InstallSnapshotResponse, LogEntry, Message, PreVoteRequest, PreVoteResponse,
    RequestVoteRequest, RequestVoteResponse, TimeoutNowRequest,
};
use thiserror::Error;

/// Maximum message size (from helix-core).
/// Safe cast: `MAX_RAFT_MESSAGE_BYTES` is 16 MB which fits in u32.
#[allow(clippy::cast_possible_truncation)]
const MAX_MESSAGE_SIZE: u32 = MAX_RAFT_MESSAGE_BYTES as u32;

/// Message type tags.
const TAG_PRE_VOTE: u8 = 0;
const TAG_PRE_VOTE_RESPONSE: u8 = 1;
const TAG_REQUEST_VOTE: u8 = 2;
const TAG_REQUEST_VOTE_RESPONSE: u8 = 3;
const TAG_APPEND_ENTRIES: u8 = 4;
const TAG_APPEND_ENTRIES_RESPONSE: u8 = 5;
const TAG_TIMEOUT_NOW: u8 = 6;
const TAG_GROUP_MESSAGE_BATCH: u8 = 7;
const TAG_INSTALL_SNAPSHOT: u8 = 8;
const TAG_INSTALL_SNAPSHOT_RESPONSE: u8 = 9;

/// Codec errors.
#[derive(Debug, Error)]
pub enum CodecError {
    /// Message exceeds maximum allowed size.
    #[error("message too large: {size} bytes (max {max})")]
    MessageTooLarge {
        /// Actual size.
        size: u32,
        /// Maximum allowed.
        max: u32,
    },

    /// Unknown message type tag.
    #[error("unknown message type: {tag}")]
    UnknownMessageType {
        /// The unknown tag value.
        tag: u8,
    },

    /// Insufficient data to decode message.
    #[error("insufficient data: need {need} bytes, have {have}")]
    InsufficientData {
        /// Bytes needed.
        need: usize,
        /// Bytes available.
        have: usize,
    },

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for codec operations.
pub type CodecResult<T> = Result<T, CodecError>;

/// Encodes a Raft message to bytes.
///
/// # Errors
/// Returns an error if the message is too large.
pub fn encode_message(message: &Message) -> CodecResult<Bytes> {
    let mut buf = BytesMut::with_capacity(256);

    // Reserve space for length prefix (filled in at the end).
    buf.put_u32_le(0);

    match message {
        Message::PreVote(req) => {
            buf.put_u8(TAG_PRE_VOTE);
            encode_pre_vote(&mut buf, req);
        }
        Message::PreVoteResponse(resp) => {
            buf.put_u8(TAG_PRE_VOTE_RESPONSE);
            encode_pre_vote_response(&mut buf, resp);
        }
        Message::RequestVote(req) => {
            buf.put_u8(TAG_REQUEST_VOTE);
            encode_request_vote(&mut buf, req);
        }
        Message::RequestVoteResponse(resp) => {
            buf.put_u8(TAG_REQUEST_VOTE_RESPONSE);
            encode_request_vote_response(&mut buf, resp);
        }
        Message::AppendEntries(req) => {
            buf.put_u8(TAG_APPEND_ENTRIES);
            encode_append_entries(&mut buf, req);
        }
        Message::AppendEntriesResponse(resp) => {
            buf.put_u8(TAG_APPEND_ENTRIES_RESPONSE);
            encode_append_entries_response(&mut buf, resp);
        }
        Message::TimeoutNow(req) => {
            buf.put_u8(TAG_TIMEOUT_NOW);
            encode_timeout_now(&mut buf, req);
        }
        Message::InstallSnapshot(req) => {
            buf.put_u8(TAG_INSTALL_SNAPSHOT);
            encode_install_snapshot(&mut buf, req);
        }
        Message::InstallSnapshotResponse(resp) => {
            buf.put_u8(TAG_INSTALL_SNAPSHOT_RESPONSE);
            encode_install_snapshot_response(&mut buf, resp);
        }
    }

    // Fill in length (excluding the 4-byte header).
    // Safe cast: message size is bounded by MAX_MESSAGE_SIZE which fits in u32.
    #[allow(clippy::cast_possible_truncation)]
    let len = (buf.len() - 4) as u32;
    if len > MAX_MESSAGE_SIZE {
        return Err(CodecError::MessageTooLarge {
            size: len,
            max: MAX_MESSAGE_SIZE,
        });
    }

    // Write length at the beginning.
    buf[0..4].copy_from_slice(&len.to_le_bytes());

    Ok(buf.freeze())
}

/// Decodes a Raft message from bytes.
///
/// Expects the full framed message including length prefix.
///
/// # Errors
/// Returns an error if the data is malformed or incomplete.
pub fn decode_message(data: &[u8]) -> CodecResult<(Message, usize)> {
    if data.len() < 4 {
        return Err(CodecError::InsufficientData {
            need: 4,
            have: data.len(),
        });
    }

    let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

    // Safe cast: len is bounded by MAX_MESSAGE_SIZE which fits in u32.
    #[allow(clippy::cast_possible_truncation)]
    let len_u32 = len as u32;
    if len > MAX_MESSAGE_SIZE as usize {
        return Err(CodecError::MessageTooLarge {
            size: len_u32,
            max: MAX_MESSAGE_SIZE,
        });
    }

    let total_len = 4 + len;
    if data.len() < total_len {
        return Err(CodecError::InsufficientData {
            need: total_len,
            have: data.len(),
        });
    }

    let payload = &data[4..total_len];
    if payload.is_empty() {
        return Err(CodecError::InsufficientData {
            need: 1,
            have: 0,
        });
    }

    let tag = payload[0];
    let body = &payload[1..];
    let mut buf = body;

    let message = match tag {
        TAG_PRE_VOTE => Message::PreVote(decode_pre_vote(&mut buf)?),
        TAG_PRE_VOTE_RESPONSE => Message::PreVoteResponse(decode_pre_vote_response(&mut buf)?),
        TAG_REQUEST_VOTE => Message::RequestVote(decode_request_vote(&mut buf)?),
        TAG_REQUEST_VOTE_RESPONSE => {
            Message::RequestVoteResponse(decode_request_vote_response(&mut buf)?)
        }
        TAG_APPEND_ENTRIES => Message::AppendEntries(decode_append_entries(&mut buf)?),
        TAG_APPEND_ENTRIES_RESPONSE => {
            Message::AppendEntriesResponse(decode_append_entries_response(&mut buf)?)
        }
        TAG_TIMEOUT_NOW => Message::TimeoutNow(decode_timeout_now(&mut buf)?),
        TAG_INSTALL_SNAPSHOT => Message::InstallSnapshot(decode_install_snapshot(&mut buf)?),
        TAG_INSTALL_SNAPSHOT_RESPONSE => {
            Message::InstallSnapshotResponse(decode_install_snapshot_response(&mut buf)?)
        }
        _ => return Err(CodecError::UnknownMessageType { tag }),
    };

    Ok((message, total_len))
}

/// Encodes a `PreVoteRequest`.
fn encode_pre_vote(buf: &mut BytesMut, req: &PreVoteRequest) {
    buf.put_u64_le(req.term.get());
    buf.put_u64_le(req.candidate_id.get());
    buf.put_u64_le(req.to.get());
    buf.put_u64_le(req.last_log_index.get());
    buf.put_u64_le(req.last_log_term.get());
}

/// Decodes a `PreVoteRequest`.
fn decode_pre_vote(buf: &mut &[u8]) -> CodecResult<PreVoteRequest> {
    ensure_remaining(buf, 40)?;

    let term = TermId::new(buf.get_u64_le());
    let candidate_id = NodeId::new(buf.get_u64_le());
    let to = NodeId::new(buf.get_u64_le());
    let last_log_index = LogIndex::new(buf.get_u64_le());
    let last_log_term = TermId::new(buf.get_u64_le());

    Ok(PreVoteRequest::new(
        term,
        candidate_id,
        to,
        last_log_index,
        last_log_term,
    ))
}

/// Encodes a `PreVoteResponse`.
fn encode_pre_vote_response(buf: &mut BytesMut, resp: &PreVoteResponse) {
    buf.put_u64_le(resp.term.get());
    buf.put_u64_le(resp.from.get());
    buf.put_u64_le(resp.to.get());
    buf.put_u8(u8::from(resp.vote_granted));
}

/// Decodes a `PreVoteResponse`.
fn decode_pre_vote_response(buf: &mut &[u8]) -> CodecResult<PreVoteResponse> {
    ensure_remaining(buf, 25)?;

    let term = TermId::new(buf.get_u64_le());
    let from = NodeId::new(buf.get_u64_le());
    let to = NodeId::new(buf.get_u64_le());
    let vote_granted = buf.get_u8() != 0;

    Ok(PreVoteResponse::new(term, from, to, vote_granted))
}

/// Encodes a `RequestVoteRequest`.
fn encode_request_vote(buf: &mut BytesMut, req: &RequestVoteRequest) {
    buf.put_u64_le(req.term.get());
    buf.put_u64_le(req.candidate_id.get());
    buf.put_u64_le(req.to.get());
    buf.put_u64_le(req.last_log_index.get());
    buf.put_u64_le(req.last_log_term.get());
}

/// Decodes a `RequestVoteRequest`.
fn decode_request_vote(buf: &mut &[u8]) -> CodecResult<RequestVoteRequest> {
    ensure_remaining(buf, 40)?;

    let term = TermId::new(buf.get_u64_le());
    let candidate_id = NodeId::new(buf.get_u64_le());
    let to = NodeId::new(buf.get_u64_le());
    let last_log_index = LogIndex::new(buf.get_u64_le());
    let last_log_term = TermId::new(buf.get_u64_le());

    Ok(RequestVoteRequest::new(
        term,
        candidate_id,
        to,
        last_log_index,
        last_log_term,
    ))
}

/// Encodes a `RequestVoteResponse`.
fn encode_request_vote_response(buf: &mut BytesMut, resp: &RequestVoteResponse) {
    buf.put_u64_le(resp.term.get());
    buf.put_u64_le(resp.from.get());
    buf.put_u64_le(resp.to.get());
    buf.put_u8(u8::from(resp.vote_granted));
}

/// Decodes a `RequestVoteResponse`.
fn decode_request_vote_response(buf: &mut &[u8]) -> CodecResult<RequestVoteResponse> {
    ensure_remaining(buf, 25)?;

    let term = TermId::new(buf.get_u64_le());
    let from = NodeId::new(buf.get_u64_le());
    let to = NodeId::new(buf.get_u64_le());
    let vote_granted = buf.get_u8() != 0;

    Ok(RequestVoteResponse::new(term, from, to, vote_granted))
}

/// Encodes an `AppendEntriesRequest`.
fn encode_append_entries(buf: &mut BytesMut, req: &AppendEntriesRequest) {
    buf.put_u64_le(req.term.get());
    buf.put_u64_le(req.leader_id.get());
    buf.put_u64_le(req.to.get());
    buf.put_u64_le(req.prev_log_index.get());
    buf.put_u64_le(req.prev_log_term.get());
    buf.put_u64_le(req.leader_commit.get());

    // Encode entries count and entries.
    // Safe cast: entries count is bounded by batch limits which fit in u32.
    #[allow(clippy::cast_possible_truncation)]
    let entry_count = req.entries.len() as u32;
    buf.put_u32_le(entry_count);
    for entry in &req.entries {
        encode_log_entry(buf, entry);
    }
}

/// Decodes an `AppendEntriesRequest`.
fn decode_append_entries(buf: &mut &[u8]) -> CodecResult<AppendEntriesRequest> {
    ensure_remaining(buf, 52)?;

    let term = TermId::new(buf.get_u64_le());
    let leader_id = NodeId::new(buf.get_u64_le());
    let to = NodeId::new(buf.get_u64_le());
    let prev_log_index = LogIndex::new(buf.get_u64_le());
    let prev_log_term = TermId::new(buf.get_u64_le());
    let leader_commit = LogIndex::new(buf.get_u64_le());

    let entry_count = buf.get_u32_le() as usize;
    let mut entries = Vec::with_capacity(entry_count);

    for _ in 0..entry_count {
        entries.push(decode_log_entry(buf)?);
    }

    Ok(AppendEntriesRequest::new(
        term,
        leader_id,
        to,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit,
    ))
}

/// Encodes an `AppendEntriesResponse`.
fn encode_append_entries_response(buf: &mut BytesMut, resp: &AppendEntriesResponse) {
    buf.put_u64_le(resp.term.get());
    buf.put_u64_le(resp.from.get());
    buf.put_u64_le(resp.to.get());
    buf.put_u8(u8::from(resp.success));
    buf.put_u64_le(resp.match_index.get());
}

/// Decodes an `AppendEntriesResponse`.
fn decode_append_entries_response(buf: &mut &[u8]) -> CodecResult<AppendEntriesResponse> {
    ensure_remaining(buf, 33)?;

    let term = TermId::new(buf.get_u64_le());
    let from = NodeId::new(buf.get_u64_le());
    let to = NodeId::new(buf.get_u64_le());
    let success = buf.get_u8() != 0;
    let match_index = LogIndex::new(buf.get_u64_le());

    Ok(AppendEntriesResponse::new(
        term,
        from,
        to,
        success,
        match_index,
    ))
}

/// Encodes a `TimeoutNowRequest`.
fn encode_timeout_now(buf: &mut BytesMut, req: &TimeoutNowRequest) {
    buf.put_u64_le(req.term.get());
    buf.put_u64_le(req.from.get());
    buf.put_u64_le(req.to.get());
}

/// Decodes a `TimeoutNowRequest`.
fn decode_timeout_now(buf: &mut &[u8]) -> CodecResult<TimeoutNowRequest> {
    ensure_remaining(buf, 24)?;

    let term = TermId::new(buf.get_u64_le());
    let from = NodeId::new(buf.get_u64_le());
    let to = NodeId::new(buf.get_u64_le());

    Ok(TimeoutNowRequest::new(term, from, to))
}

/// Encodes an `InstallSnapshotRequest`.
fn encode_install_snapshot(buf: &mut BytesMut, req: &InstallSnapshotRequest) {
    buf.put_u64_le(req.term.get());
    buf.put_u64_le(req.leader_id.get());
    buf.put_u64_le(req.to.get());
    buf.put_u64_le(req.last_included_index.get());
    buf.put_u64_le(req.last_included_term.get());
    buf.put_u64_le(req.offset);
    // Safe cast: snapshot data size is bounded by message limits which fit in u32.
    #[allow(clippy::cast_possible_truncation)]
    let data_len = req.data.len() as u32;
    buf.put_u32_le(data_len);
    buf.put_slice(&req.data);
    buf.put_u8(u8::from(req.done));
}

/// Decodes an `InstallSnapshotRequest`.
fn decode_install_snapshot(buf: &mut &[u8]) -> CodecResult<InstallSnapshotRequest> {
    ensure_remaining(buf, 52)?;

    let term = TermId::new(buf.get_u64_le());
    let leader_id = NodeId::new(buf.get_u64_le());
    let to = NodeId::new(buf.get_u64_le());
    let last_included_index = LogIndex::new(buf.get_u64_le());
    let last_included_term = TermId::new(buf.get_u64_le());
    let offset = buf.get_u64_le();
    let data_len = buf.get_u32_le() as usize;

    ensure_remaining(buf, data_len + 1)?;
    let data = Bytes::copy_from_slice(&buf[..data_len]);
    buf.advance(data_len);
    let done = buf.get_u8() != 0;

    Ok(InstallSnapshotRequest::new(
        term,
        leader_id,
        to,
        last_included_index,
        last_included_term,
        offset,
        data,
        done,
    ))
}

/// Encodes an `InstallSnapshotResponse`.
fn encode_install_snapshot_response(buf: &mut BytesMut, resp: &InstallSnapshotResponse) {
    buf.put_u64_le(resp.term.get());
    buf.put_u64_le(resp.from.get());
    buf.put_u64_le(resp.to.get());
    buf.put_u8(u8::from(resp.success));
    buf.put_u64_le(resp.next_offset);
}

/// Decodes an `InstallSnapshotResponse`.
fn decode_install_snapshot_response(buf: &mut &[u8]) -> CodecResult<InstallSnapshotResponse> {
    ensure_remaining(buf, 33)?;

    let term = TermId::new(buf.get_u64_le());
    let from = NodeId::new(buf.get_u64_le());
    let to = NodeId::new(buf.get_u64_le());
    let success = buf.get_u8() != 0;
    let next_offset = buf.get_u64_le();

    Ok(InstallSnapshotResponse::new(term, from, to, success, next_offset))
}

/// Encodes a log entry.
fn encode_log_entry(buf: &mut BytesMut, entry: &LogEntry) {
    buf.put_u64_le(entry.term.get());
    buf.put_u64_le(entry.index.get());
    // Safe cast: entry data size is bounded by message limits which fit in u32.
    #[allow(clippy::cast_possible_truncation)]
    let data_len = entry.data.len() as u32;
    buf.put_u32_le(data_len);
    buf.put_slice(&entry.data);
}

/// Decodes a log entry.
fn decode_log_entry(buf: &mut &[u8]) -> CodecResult<LogEntry> {
    ensure_remaining(buf, 20)?;

    let term = TermId::new(buf.get_u64_le());
    let index = LogIndex::new(buf.get_u64_le());
    let data_len = buf.get_u32_le() as usize;

    ensure_remaining(buf, data_len)?;
    let data = Bytes::copy_from_slice(&buf[..data_len]);
    buf.advance(data_len);

    Ok(LogEntry::new(term, index, data))
}

/// Ensures the buffer has at least `need` bytes remaining.
const fn ensure_remaining(buf: &[u8], need: usize) -> CodecResult<()> {
    if buf.len() < need {
        return Err(CodecError::InsufficientData {
            need,
            have: buf.len(),
        });
    }
    Ok(())
}

/// Encodes a batch of `GroupMessage`s to bytes.
///
/// Wire format:
/// - 4 bytes: total length (u32 little-endian, not including header)
/// - 1 byte: tag (7 = `GroupMessageBatch`)
/// - 4 bytes: message count (u32 little-endian)
/// - For each message:
///   - 8 bytes: `group_id` (u64 little-endian)
///   - N bytes: encoded message (without length prefix)
///
/// # Errors
/// Returns an error if the batch is too large.
pub fn encode_group_batch(messages: &[GroupMessage]) -> CodecResult<Bytes> {
    // Estimate initial capacity: 4 (length) + 1 (tag) + 4 (count) + messages.
    let capacity = 9 + messages.len() * 64;
    let mut buf = BytesMut::with_capacity(capacity);

    // Reserve space for length prefix.
    buf.put_u32_le(0);

    // Tag for batch.
    buf.put_u8(TAG_GROUP_MESSAGE_BATCH);

    // Message count.
    // Safe cast: count bounded by practical limits.
    #[allow(clippy::cast_possible_truncation)]
    let count = messages.len() as u32;
    buf.put_u32_le(count);

    // Encode each GroupMessage.
    for group_msg in messages {
        buf.put_u64_le(group_msg.group_id.get());
        encode_message_payload(&mut buf, &group_msg.message);
    }

    // Fill in total length (excluding the 4-byte header).
    // Safe cast: message size bounded by MAX_MESSAGE_SIZE.
    #[allow(clippy::cast_possible_truncation)]
    let len = (buf.len() - 4) as u32;
    if len > MAX_MESSAGE_SIZE {
        return Err(CodecError::MessageTooLarge {
            size: len,
            max: MAX_MESSAGE_SIZE,
        });
    }

    buf[0..4].copy_from_slice(&len.to_le_bytes());

    Ok(buf.freeze())
}

/// Encodes message payload (without length prefix).
fn encode_message_payload(buf: &mut BytesMut, message: &Message) {
    match message {
        Message::PreVote(req) => {
            buf.put_u8(TAG_PRE_VOTE);
            encode_pre_vote(buf, req);
        }
        Message::PreVoteResponse(resp) => {
            buf.put_u8(TAG_PRE_VOTE_RESPONSE);
            encode_pre_vote_response(buf, resp);
        }
        Message::RequestVote(req) => {
            buf.put_u8(TAG_REQUEST_VOTE);
            encode_request_vote(buf, req);
        }
        Message::RequestVoteResponse(resp) => {
            buf.put_u8(TAG_REQUEST_VOTE_RESPONSE);
            encode_request_vote_response(buf, resp);
        }
        Message::AppendEntries(req) => {
            buf.put_u8(TAG_APPEND_ENTRIES);
            encode_append_entries(buf, req);
        }
        Message::AppendEntriesResponse(resp) => {
            buf.put_u8(TAG_APPEND_ENTRIES_RESPONSE);
            encode_append_entries_response(buf, resp);
        }
        Message::TimeoutNow(req) => {
            buf.put_u8(TAG_TIMEOUT_NOW);
            encode_timeout_now(buf, req);
        }
        Message::InstallSnapshot(req) => {
            buf.put_u8(TAG_INSTALL_SNAPSHOT);
            encode_install_snapshot(buf, req);
        }
        Message::InstallSnapshotResponse(resp) => {
            buf.put_u8(TAG_INSTALL_SNAPSHOT_RESPONSE);
            encode_install_snapshot_response(buf, resp);
        }
    }
}

/// Decodes a batch of `GroupMessage`s from bytes.
///
/// Expects the full framed batch including length prefix.
///
/// # Errors
/// Returns an error if the data is malformed or incomplete.
///
/// # Panics
/// Panics if the decoded message count doesn't match the expected count (indicates a bug).
pub fn decode_group_batch(data: &[u8]) -> CodecResult<(Vec<GroupMessage>, usize)> {
    if data.len() < 4 {
        return Err(CodecError::InsufficientData {
            need: 4,
            have: data.len(),
        });
    }

    let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

    // Safe cast: len bounded by MAX_MESSAGE_SIZE.
    #[allow(clippy::cast_possible_truncation)]
    let len_u32 = len as u32;
    if len > MAX_MESSAGE_SIZE as usize {
        return Err(CodecError::MessageTooLarge {
            size: len_u32,
            max: MAX_MESSAGE_SIZE,
        });
    }

    let total_len = 4 + len;
    if data.len() < total_len {
        return Err(CodecError::InsufficientData {
            need: total_len,
            have: data.len(),
        });
    }

    let payload = &data[4..total_len];
    if payload.is_empty() {
        return Err(CodecError::InsufficientData {
            need: 1,
            have: 0,
        });
    }

    let tag = payload[0];
    if tag != TAG_GROUP_MESSAGE_BATCH {
        return Err(CodecError::UnknownMessageType { tag });
    }

    let body = &payload[1..];
    if body.len() < 4 {
        return Err(CodecError::InsufficientData {
            need: 5,
            have: payload.len(),
        });
    }

    let count = u32::from_le_bytes([body[0], body[1], body[2], body[3]]) as usize;
    let mut buf = &body[4..];
    let mut messages = Vec::with_capacity(count);

    for _ in 0..count {
        ensure_remaining(buf, 9)?; // 8 (group_id) + 1 (tag)

        let group_id = GroupId::new((&mut buf).get_u64_le());
        let message = decode_message_payload(&mut buf)?;

        messages.push(GroupMessage::new(group_id, message));
    }

    assert_eq!(messages.len(), count);

    Ok((messages, total_len))
}

/// Decodes message payload (without length prefix).
fn decode_message_payload(buf: &mut &[u8]) -> CodecResult<Message> {
    ensure_remaining(buf, 1)?;

    let tag = buf[0];
    *buf = &buf[1..];

    let message = match tag {
        TAG_PRE_VOTE => Message::PreVote(decode_pre_vote(buf)?),
        TAG_PRE_VOTE_RESPONSE => Message::PreVoteResponse(decode_pre_vote_response(buf)?),
        TAG_REQUEST_VOTE => Message::RequestVote(decode_request_vote(buf)?),
        TAG_REQUEST_VOTE_RESPONSE => Message::RequestVoteResponse(decode_request_vote_response(buf)?),
        TAG_APPEND_ENTRIES => Message::AppendEntries(decode_append_entries(buf)?),
        TAG_APPEND_ENTRIES_RESPONSE => {
            Message::AppendEntriesResponse(decode_append_entries_response(buf)?)
        }
        TAG_TIMEOUT_NOW => Message::TimeoutNow(decode_timeout_now(buf)?),
        _ => return Err(CodecError::UnknownMessageType { tag }),
    };

    Ok(message)
}

/// Checks if the given data starts with a group message batch tag.
///
/// Used by the transport layer to determine which decoder to use.
#[must_use]
pub fn is_group_batch(data: &[u8]) -> bool {
    // Need at least 5 bytes: 4 (length) + 1 (tag).
    data.len() >= 5 && data[4] == TAG_GROUP_MESSAGE_BATCH
}

// =============================================================================
// Transfer Message Codec
// =============================================================================

/// Transfer message type tags.
const TAG_TRANSFER_PREPARE_REQUEST: u8 = 20;
const TAG_TRANSFER_PREPARE_RESPONSE: u8 = 21;
const TAG_TRANSFER_SNAPSHOT_CHUNK: u8 = 22;
const TAG_TRANSFER_CATCHUP_ENTRIES: u8 = 23;
const TAG_TRANSFER_SWITCH_REQUEST: u8 = 24;
const TAG_TRANSFER_SWITCH_RESPONSE: u8 = 25;
const TAG_TRANSFER_CANCEL_REQUEST: u8 = 26;

/// Broker heartbeat message tag.
/// Heartbeats are soft-state messages (not Raft-replicated) sent directly between nodes.
const TAG_BROKER_HEARTBEAT: u8 = 30;

use helix_core::TransferId;
use helix_routing::{ShardRange, TransferMessage};

/// Encodes a transfer message to bytes.
///
/// Wire format:
/// - 4 bytes: message length (u32 little-endian, not including header)
/// - 1 byte: message type tag (20-26)
/// - 8 bytes: transfer ID
/// - N bytes: type-specific payload
///
/// # Errors
///
/// Returns an error if the message is too large.
pub fn encode_transfer_message(message: &TransferMessage) -> CodecResult<Bytes> {
    let mut buf = BytesMut::with_capacity(256);

    // Reserve space for length prefix.
    buf.put_u32_le(0);

    match message {
        TransferMessage::PrepareRequest {
            transfer_id,
            shard_range,
        } => {
            buf.put_u8(TAG_TRANSFER_PREPARE_REQUEST);
            buf.put_u64_le(transfer_id.get());
            buf.put_u32_le(shard_range.start);
            buf.put_u32_le(shard_range.end);
        }
        TransferMessage::PrepareResponse {
            transfer_id,
            snapshot_index,
            snapshot_term,
            snapshot_size,
        } => {
            buf.put_u8(TAG_TRANSFER_PREPARE_RESPONSE);
            buf.put_u64_le(transfer_id.get());
            buf.put_u64_le(snapshot_index.get());
            buf.put_u64_le(snapshot_term.get());
            buf.put_u64_le(*snapshot_size);
        }
        TransferMessage::SnapshotChunk {
            transfer_id,
            offset,
            data,
            done,
        } => {
            buf.put_u8(TAG_TRANSFER_SNAPSHOT_CHUNK);
            buf.put_u64_le(transfer_id.get());
            buf.put_u64_le(*offset);
            // Safe cast: data size bounded by MAX_MESSAGE_SIZE.
            #[allow(clippy::cast_possible_truncation)]
            let data_len = data.len() as u32;
            buf.put_u32_le(data_len);
            buf.put_slice(data);
            buf.put_u8(u8::from(*done));
        }
        TransferMessage::CatchupEntries {
            transfer_id,
            start_index,
            entry_count,
            entries_data,
            done,
        } => {
            buf.put_u8(TAG_TRANSFER_CATCHUP_ENTRIES);
            buf.put_u64_le(transfer_id.get());
            buf.put_u64_le(start_index.get());
            buf.put_u32_le(*entry_count);
            // Safe cast: entries_data size bounded by MAX_MESSAGE_SIZE.
            #[allow(clippy::cast_possible_truncation)]
            let data_len = entries_data.len() as u32;
            buf.put_u32_le(data_len);
            buf.put_slice(entries_data);
            buf.put_u8(u8::from(*done));
        }
        TransferMessage::SwitchRequest { transfer_id } => {
            buf.put_u8(TAG_TRANSFER_SWITCH_REQUEST);
            buf.put_u64_le(transfer_id.get());
        }
        TransferMessage::SwitchResponse {
            transfer_id,
            success,
        } => {
            buf.put_u8(TAG_TRANSFER_SWITCH_RESPONSE);
            buf.put_u64_le(transfer_id.get());
            buf.put_u8(u8::from(*success));
        }
        TransferMessage::CancelRequest {
            transfer_id,
            reason,
        } => {
            buf.put_u8(TAG_TRANSFER_CANCEL_REQUEST);
            buf.put_u64_le(transfer_id.get());
            let reason_bytes = reason.as_bytes();
            // Safe cast: reason length bounded by practical limits.
            #[allow(clippy::cast_possible_truncation)]
            let reason_len = reason_bytes.len() as u32;
            buf.put_u32_le(reason_len);
            buf.put_slice(reason_bytes);
        }
    }

    // Fill in length (excluding the 4-byte header).
    #[allow(clippy::cast_possible_truncation)]
    let len = (buf.len() - 4) as u32;
    if len > MAX_MESSAGE_SIZE {
        return Err(CodecError::MessageTooLarge {
            size: len,
            max: MAX_MESSAGE_SIZE,
        });
    }

    buf[0..4].copy_from_slice(&len.to_le_bytes());
    Ok(buf.freeze())
}

/// Decodes a transfer message from bytes.
///
/// Expects the full framed message including length prefix.
///
/// # Errors
///
/// Returns an error if the data is malformed or incomplete.
pub fn decode_transfer_message(data: &[u8]) -> CodecResult<(TransferMessage, usize)> {
    if data.len() < 4 {
        return Err(CodecError::InsufficientData {
            need: 4,
            have: data.len(),
        });
    }

    let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;

    #[allow(clippy::cast_possible_truncation)]
    let len_u32 = len as u32;
    if len > MAX_MESSAGE_SIZE as usize {
        return Err(CodecError::MessageTooLarge {
            size: len_u32,
            max: MAX_MESSAGE_SIZE,
        });
    }

    let total_len = 4 + len;
    if data.len() < total_len {
        return Err(CodecError::InsufficientData {
            need: total_len,
            have: data.len(),
        });
    }

    let payload = &data[4..total_len];
    if payload.is_empty() {
        return Err(CodecError::InsufficientData {
            need: 1,
            have: 0,
        });
    }

    let tag = payload[0];
    let mut buf = &payload[1..];
    let message = decode_transfer_payload(tag, &mut buf)?;
    Ok((message, total_len))
}

/// Decodes the payload portion of a transfer message.
fn decode_transfer_payload(tag: u8, buf: &mut &[u8]) -> CodecResult<TransferMessage> {
    match tag {
        TAG_TRANSFER_PREPARE_REQUEST => decode_transfer_prepare_request(buf),
        TAG_TRANSFER_PREPARE_RESPONSE => decode_transfer_prepare_response(buf),
        TAG_TRANSFER_SNAPSHOT_CHUNK => decode_transfer_snapshot_chunk(buf),
        TAG_TRANSFER_CATCHUP_ENTRIES => decode_transfer_catchup_entries(buf),
        TAG_TRANSFER_SWITCH_REQUEST => decode_transfer_switch_request(buf),
        TAG_TRANSFER_SWITCH_RESPONSE => decode_transfer_switch_response(buf),
        TAG_TRANSFER_CANCEL_REQUEST => decode_transfer_cancel_request(buf),
        _ => Err(CodecError::UnknownMessageType { tag }),
    }
}

fn decode_transfer_prepare_request(buf: &mut &[u8]) -> CodecResult<TransferMessage> {
    ensure_remaining(buf, 16)?;
    let transfer_id = TransferId::new(buf.get_u64_le());
    let start = buf.get_u32_le();
    let end = buf.get_u32_le();
    Ok(TransferMessage::PrepareRequest {
        transfer_id,
        shard_range: ShardRange::new(start, end),
    })
}

fn decode_transfer_prepare_response(buf: &mut &[u8]) -> CodecResult<TransferMessage> {
    ensure_remaining(buf, 32)?;
    let transfer_id = TransferId::new(buf.get_u64_le());
    let snapshot_index = LogIndex::new(buf.get_u64_le());
    let snapshot_term = TermId::new(buf.get_u64_le());
    let snapshot_size = buf.get_u64_le();
    Ok(TransferMessage::PrepareResponse {
        transfer_id,
        snapshot_index,
        snapshot_term,
        snapshot_size,
    })
}

fn decode_transfer_snapshot_chunk(buf: &mut &[u8]) -> CodecResult<TransferMessage> {
    ensure_remaining(buf, 20)?;
    let transfer_id = TransferId::new(buf.get_u64_le());
    let offset = buf.get_u64_le();
    let data_len = buf.get_u32_le() as usize;
    ensure_remaining(buf, data_len + 1)?;
    let data = Bytes::copy_from_slice(&buf[..data_len]);
    buf.advance(data_len);
    let done = buf.get_u8() != 0;
    Ok(TransferMessage::SnapshotChunk {
        transfer_id,
        offset,
        data,
        done,
    })
}

fn decode_transfer_catchup_entries(buf: &mut &[u8]) -> CodecResult<TransferMessage> {
    ensure_remaining(buf, 24)?;
    let transfer_id = TransferId::new(buf.get_u64_le());
    let start_index = LogIndex::new(buf.get_u64_le());
    let entry_count = buf.get_u32_le();
    let data_len = buf.get_u32_le() as usize;
    ensure_remaining(buf, data_len + 1)?;
    let entries_data = Bytes::copy_from_slice(&buf[..data_len]);
    buf.advance(data_len);
    let done = buf.get_u8() != 0;
    Ok(TransferMessage::CatchupEntries {
        transfer_id,
        start_index,
        entry_count,
        entries_data,
        done,
    })
}

fn decode_transfer_switch_request(buf: &mut &[u8]) -> CodecResult<TransferMessage> {
    ensure_remaining(buf, 8)?;
    let transfer_id = TransferId::new(buf.get_u64_le());
    Ok(TransferMessage::SwitchRequest { transfer_id })
}

fn decode_transfer_switch_response(buf: &mut &[u8]) -> CodecResult<TransferMessage> {
    ensure_remaining(buf, 9)?;
    let transfer_id = TransferId::new(buf.get_u64_le());
    let success = buf.get_u8() != 0;
    Ok(TransferMessage::SwitchResponse {
        transfer_id,
        success,
    })
}

fn decode_transfer_cancel_request(buf: &mut &[u8]) -> CodecResult<TransferMessage> {
    ensure_remaining(buf, 12)?;
    let transfer_id = TransferId::new(buf.get_u64_le());
    let reason_len = buf.get_u32_le() as usize;
    ensure_remaining(buf, reason_len)?;
    let reason = String::from_utf8_lossy(&buf[..reason_len]).into_owned();
    Ok(TransferMessage::CancelRequest {
        transfer_id,
        reason,
    })
}

/// Checks if the given data starts with a transfer message tag.
#[must_use]
pub fn is_transfer_message(data: &[u8]) -> bool {
    // Need at least 5 bytes: 4 (length) + 1 (tag).
    data.len() >= 5 && (TAG_TRANSFER_PREPARE_REQUEST..=TAG_TRANSFER_CANCEL_REQUEST).contains(&data[4])
}

// =============================================================================
// Broker Heartbeat Codec
// =============================================================================

/// Broker heartbeat message.
///
/// Unlike other controller state, heartbeats are **soft state** (not Raft-replicated).
/// Each broker sends heartbeats directly to all peers via transport. Each node
/// maintains its own local view of broker liveness based on received heartbeats.
///
/// This follows the Kafka `KRaft` pattern where `BrokerHeartbeatRequest` is a
/// separate RPC mechanism, not part of Raft replication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerHeartbeat {
    /// The broker's node ID.
    pub node_id: NodeId,
    /// Timestamp in milliseconds when this heartbeat was sent.
    pub timestamp_ms: u64,
}

impl BrokerHeartbeat {
    /// Creates a new broker heartbeat.
    #[must_use]
    pub const fn new(node_id: NodeId, timestamp_ms: u64) -> Self {
        Self {
            node_id,
            timestamp_ms,
        }
    }
}

/// Encodes a broker heartbeat to bytes.
///
/// Wire format:
/// - 4 bytes: message length (u32 little-endian, not including header)
/// - 1 byte: tag (30)
/// - 8 bytes: node ID (u64 little-endian)
/// - 8 bytes: `timestamp_ms` (u64 little-endian)
///
/// # Errors
///
/// Returns an error if encoding fails (should not happen for heartbeats).
pub fn encode_broker_heartbeat(heartbeat: &BrokerHeartbeat) -> CodecResult<Bytes> {
    let mut buf = BytesMut::with_capacity(21); // 4 + 1 + 8 + 8

    // Reserve space for length prefix.
    buf.put_u32_le(0);

    buf.put_u8(TAG_BROKER_HEARTBEAT);
    buf.put_u64_le(heartbeat.node_id.get());
    buf.put_u64_le(heartbeat.timestamp_ms);

    // Fill in length (excluding the 4-byte header).
    #[allow(clippy::cast_possible_truncation)]
    let len = (buf.len() - 4) as u32;
    buf[0..4].copy_from_slice(&len.to_le_bytes());

    Ok(buf.freeze())
}

/// Decodes a broker heartbeat from bytes.
///
/// Expects the full framed message including length prefix.
///
/// # Errors
///
/// Returns an error if the data is malformed or incomplete.
pub fn decode_broker_heartbeat(data: &[u8]) -> CodecResult<(BrokerHeartbeat, usize)> {
    if data.len() < 4 {
        return Err(CodecError::InsufficientData {
            need: 4,
            have: data.len(),
        });
    }

    let len = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let total_len = 4 + len;

    if data.len() < total_len {
        return Err(CodecError::InsufficientData {
            need: total_len,
            have: data.len(),
        });
    }

    let payload = &data[4..total_len];
    if payload.len() < 17 {
        // 1 (tag) + 8 (node_id) + 8 (timestamp_ms)
        return Err(CodecError::InsufficientData {
            need: 17,
            have: payload.len(),
        });
    }

    let tag = payload[0];
    if tag != TAG_BROKER_HEARTBEAT {
        return Err(CodecError::UnknownMessageType { tag });
    }

    let mut buf = &payload[1..];
    let node_id = NodeId::new(buf.get_u64_le());
    let timestamp_ms = buf.get_u64_le();

    Ok((BrokerHeartbeat::new(node_id, timestamp_ms), total_len))
}

/// Checks if the given data starts with a broker heartbeat tag.
#[must_use]
pub fn is_broker_heartbeat(data: &[u8]) -> bool {
    // Need at least 5 bytes: 4 (length) + 1 (tag).
    data.len() >= 5 && data[4] == TAG_BROKER_HEARTBEAT
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request_vote() -> Message {
        Message::RequestVote(RequestVoteRequest::new(
            TermId::new(5),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(10),
            TermId::new(4),
        ))
    }

    fn make_request_vote_response() -> Message {
        Message::RequestVoteResponse(RequestVoteResponse::new(
            TermId::new(5),
            NodeId::new(2),
            NodeId::new(1),
            true,
        ))
    }

    fn make_append_entries_heartbeat() -> Message {
        Message::AppendEntries(AppendEntriesRequest::heartbeat(
            TermId::new(5),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(10),
            TermId::new(4),
            LogIndex::new(8),
        ))
    }

    fn make_append_entries_with_data() -> Message {
        let entries = vec![
            LogEntry::new(TermId::new(5), LogIndex::new(11), Bytes::from("hello")),
            LogEntry::new(TermId::new(5), LogIndex::new(12), Bytes::from("world")),
        ];
        Message::AppendEntries(AppendEntriesRequest::new(
            TermId::new(5),
            NodeId::new(1),
            NodeId::new(2),
            LogIndex::new(10),
            TermId::new(4),
            entries,
            LogIndex::new(8),
        ))
    }

    fn make_append_entries_response() -> Message {
        Message::AppendEntriesResponse(AppendEntriesResponse::new(
            TermId::new(5),
            NodeId::new(2),
            NodeId::new(1),
            true,
            LogIndex::new(12),
        ))
    }

    #[test]
    fn test_encode_decode_request_vote() {
        let original = make_request_vote();
        let encoded = encode_message(&original).unwrap();
        let (decoded, consumed) = decode_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_request_vote_response() {
        let original = make_request_vote_response();
        let encoded = encode_message(&original).unwrap();
        let (decoded, consumed) = decode_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_append_entries_heartbeat() {
        let original = make_append_entries_heartbeat();
        let encoded = encode_message(&original).unwrap();
        let (decoded, consumed) = decode_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_append_entries_with_data() {
        let original = make_append_entries_with_data();
        let encoded = encode_message(&original).unwrap();
        let (decoded, consumed) = decode_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_append_entries_response() {
        let original = make_append_entries_response();
        let encoded = encode_message(&original).unwrap();
        let (decoded, consumed) = decode_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_insufficient_data() {
        // Only 2 bytes when we need at least 4.
        let result = decode_message(&[0, 1]);
        assert!(matches!(result, Err(CodecError::InsufficientData { .. })));
    }

    #[test]
    fn test_unknown_message_type() {
        // Valid length but unknown tag.
        let data = [1, 0, 0, 0, 255]; // length=1, tag=255
        let result = decode_message(&data);
        assert!(matches!(result, Err(CodecError::UnknownMessageType { tag: 255 })));
    }

    #[test]
    fn test_encode_decode_group_batch_empty() {
        let messages: Vec<GroupMessage> = vec![];
        let encoded = encode_group_batch(&messages).unwrap();
        let (decoded, consumed) = decode_group_batch(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn test_encode_decode_group_batch_single() {
        let messages = vec![GroupMessage::new(
            GroupId::new(42),
            make_request_vote(),
        )];
        let encoded = encode_group_batch(&messages).unwrap();
        let (decoded, consumed) = decode_group_batch(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].group_id.get(), 42);
        assert_eq!(decoded[0].message, make_request_vote());
    }

    #[test]
    fn test_encode_decode_group_batch_multiple() {
        let messages = vec![
            GroupMessage::new(GroupId::new(1), make_request_vote()),
            GroupMessage::new(GroupId::new(2), make_request_vote_response()),
            GroupMessage::new(GroupId::new(3), make_append_entries_heartbeat()),
            GroupMessage::new(GroupId::new(1), make_append_entries_with_data()),
            GroupMessage::new(GroupId::new(2), make_append_entries_response()),
        ];
        let encoded = encode_group_batch(&messages).unwrap();
        let (decoded, consumed) = decode_group_batch(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded.len(), 5);

        assert_eq!(decoded[0].group_id.get(), 1);
        assert_eq!(decoded[0].message, make_request_vote());

        assert_eq!(decoded[1].group_id.get(), 2);
        assert_eq!(decoded[1].message, make_request_vote_response());

        assert_eq!(decoded[2].group_id.get(), 3);
        assert_eq!(decoded[2].message, make_append_entries_heartbeat());

        assert_eq!(decoded[3].group_id.get(), 1);
        assert_eq!(decoded[3].message, make_append_entries_with_data());

        assert_eq!(decoded[4].group_id.get(), 2);
        assert_eq!(decoded[4].message, make_append_entries_response());
    }

    #[test]
    fn test_is_group_batch() {
        // Regular message.
        let regular = encode_message(&make_request_vote()).unwrap();
        assert!(!is_group_batch(&regular));

        // Group batch.
        let batch = encode_group_batch(&[GroupMessage::new(
            GroupId::new(1),
            make_request_vote(),
        )])
        .unwrap();
        assert!(is_group_batch(&batch));

        // Empty batch.
        let empty_batch = encode_group_batch(&[]).unwrap();
        assert!(is_group_batch(&empty_batch));

        // Too short to determine.
        assert!(!is_group_batch(&[0, 1, 2, 3]));
        assert!(!is_group_batch(&[]));
    }

    #[test]
    fn test_decode_group_batch_insufficient_data() {
        let result = decode_group_batch(&[0, 1]);
        assert!(matches!(result, Err(CodecError::InsufficientData { .. })));
    }

    #[test]
    fn test_decode_group_batch_wrong_tag() {
        // Encode a regular message and try to decode as batch.
        let regular = encode_message(&make_request_vote()).unwrap();
        let result = decode_group_batch(&regular);
        assert!(matches!(result, Err(CodecError::UnknownMessageType { .. })));
    }

    // =========================================================================
    // Transfer Message Codec Tests
    // =========================================================================

    #[test]
    fn test_encode_decode_transfer_prepare_request() {
        let original = TransferMessage::PrepareRequest {
            transfer_id: TransferId::new(42),
            shard_range: ShardRange::new(100, 500),
        };
        let encoded = encode_transfer_message(&original).unwrap();
        let (decoded, consumed) = decode_transfer_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_transfer_prepare_response() {
        let original = TransferMessage::PrepareResponse {
            transfer_id: TransferId::new(42),
            snapshot_index: LogIndex::new(1000),
            snapshot_term: TermId::new(5),
            snapshot_size: 1024 * 1024,
        };
        let encoded = encode_transfer_message(&original).unwrap();
        let (decoded, consumed) = decode_transfer_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_transfer_snapshot_chunk() {
        let original = TransferMessage::SnapshotChunk {
            transfer_id: TransferId::new(42),
            offset: 4096,
            data: Bytes::from("snapshot data here"),
            done: false,
        };
        let encoded = encode_transfer_message(&original).unwrap();
        let (decoded, consumed) = decode_transfer_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_transfer_snapshot_chunk_done() {
        let original = TransferMessage::SnapshotChunk {
            transfer_id: TransferId::new(42),
            offset: 8192,
            data: Bytes::from("final chunk"),
            done: true,
        };
        let encoded = encode_transfer_message(&original).unwrap();
        let (decoded, consumed) = decode_transfer_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_transfer_catchup_entries() {
        let original = TransferMessage::CatchupEntries {
            transfer_id: TransferId::new(42),
            start_index: LogIndex::new(1001),
            entry_count: 50,
            entries_data: Bytes::from("serialized entries"),
            done: false,
        };
        let encoded = encode_transfer_message(&original).unwrap();
        let (decoded, consumed) = decode_transfer_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_transfer_switch_request() {
        let original = TransferMessage::SwitchRequest {
            transfer_id: TransferId::new(42),
        };
        let encoded = encode_transfer_message(&original).unwrap();
        let (decoded, consumed) = decode_transfer_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_transfer_switch_response() {
        let original = TransferMessage::SwitchResponse {
            transfer_id: TransferId::new(42),
            success: true,
        };
        let encoded = encode_transfer_message(&original).unwrap();
        let (decoded, consumed) = decode_transfer_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_encode_decode_transfer_cancel_request() {
        let original = TransferMessage::CancelRequest {
            transfer_id: TransferId::new(42),
            reason: "timeout exceeded".to_string(),
        };
        let encoded = encode_transfer_message(&original).unwrap();
        let (decoded, consumed) = decode_transfer_message(&encoded).unwrap();

        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_is_transfer_message() {
        // Transfer message.
        let transfer = encode_transfer_message(&TransferMessage::SwitchRequest {
            transfer_id: TransferId::new(1),
        })
        .unwrap();
        assert!(is_transfer_message(&transfer));

        // Regular Raft message - not a transfer message.
        let raft = encode_message(&make_request_vote()).unwrap();
        assert!(!is_transfer_message(&raft));

        // Too short.
        assert!(!is_transfer_message(&[0, 1, 2, 3]));
        assert!(!is_transfer_message(&[]));
    }
}
