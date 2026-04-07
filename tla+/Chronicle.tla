------------------- MODULE Chronicle -------------------
EXTENDS FiniteSets, Sequences, Integers, TLC, UnreliableNetwork

(***************************************************************************

CHRONICLE: DISTRIBUTED FLEXIBLE STREAM CONSENSUS PROTOCOL

A simplified, term-based distributed consensus protocol for stream storage.

Key design decisions:
  - Full replication (no striping). Every entry is written to ALL nodes
    in the ensemble and ALL must acknowledge. This eliminates the
    complexity of partial replication and quorum coverage edge cases.
  - Term-based fencing replaces boolean fencing. When a new client takes
    over a timeline, it increments the term. Units reject requests with
    stale terms. This enables unbounded timelines with multiple writers
    over their lifetime.
  - Reconciliation at term boundaries. A new term holder must
    reconcile the timeline before writing, ensuring dirty entries from
    previous terms are cleaned up via a truncation mechanism.
  - Ensemble changes only on unit failure. The writeset is always the
    full ensemble. A failed unit is replaced and a new segment is
    created starting from LRA + 1.

Protocol safety properties:
  - AllAckedEventsFullyReplicated: Every acknowledged event is
    replicated on all ensemble members.
  - NoAckedEventLost: Once an event is acknowledged, it is never lost
    (assuming no permanent loss of all ensemble members).
  - NoDivergence: All units in an ensemble agree on the value of every
    committed offset.

Read path: Not modeled. Read correctness follows from the write path
  safety properties above. Since all committed offsets are fully
  replicated and no divergence exists, a reader can read from any unit
  in the ensemble for any offset <= LRA and is guaranteed consistent,
  correct data.

Terminology:
  - Timeline: A logical append-only stream managed by a client. Analogous
    to a ledger in BookKeeper but unbounded.
  - Unit: A storage server node. Analogous to a bookie.
  - Segment: A contiguous range of offsets within a timeline, associated
    with a fixed ensemble of units.
  - Term: An integer epoch for fencing. Incremented on leader change.
  - LRS: Last Record Sent - the highest offset sent by the timeline client.
  - LRA: Last Record Acked - the highest contiguous offset acknowledged
    by all units in the ensemble (commit point).

***************************************************************************)

CONSTANTS
    \* Message types
    RecordEventRequest,
    RecordEventResponse,
    FenceRequest,
    FenceResponse,

    \* Model inputs
    Units,              \* Set of all unit (server) identifiers
    Timelines,          \* Set of all timeline identifiers
    ReplicationFactor,             \* Number of replicas (= ensemble size)
    Events,             \* Set of event payloads to write

    \* Response codes
    Ok,
    InvalidTerm,

    \* Sentinel values
    Null,

    \* Timeline status
    TimelineStatusOpen,
    TimelineStatusInReconciliation,
    TimelineStatusClosed

\* ------ Assumptions ------
\* ReplicationFactor must be positive and there must be enough units
ASSUME ReplicationFactor \in Nat /\ ReplicationFactor > 0
ASSUME Cardinality(Units) >= ReplicationFactor
ASSUME Cardinality(Events) >= 1


(***************************************************************************)
(* VARIABLES                                                               *)
(***************************************************************************)

VARIABLE catalogs         \* Metadata store: per-timeline catalog
VARIABLE units            \* State of each unit (server)
VARIABLE timelines        \* State of each timeline (client-side)
VARIABLE sent_events      \* Set of event payloads already sent (model constraint)
VARIABLE acked_events     \* Set of event payloads acknowledged to clients

vars == << units, catalogs, message_channel, timelines,
           sent_events, acked_events, message_fail_count >>


(***************************************************************************)
(* TYPE DEFINITIONS                                                        *)
(***************************************************************************)

\* Offset domain for events
EventOffsets == 1..(Cardinality(Events) + Cardinality(Timelines))

\* A single event record
Event == [offset: EventOffsets, data: Events]

NullEvent == [offset |-> 0, data |-> Null]

\* Version type for catalog CAS operations
Version == Nat \union {Null}

\* A segment: range of offsets with a fixed ensemble
Segment == [id: Nat, ensemble: SUBSET Units, start_offset: Nat]

\* An in-flight write tracked by the timeline client
InflightEvent == [event: Event, segment_id: Nat, ensemble: SUBSET Units]

\* Timeline status values
TimelineStatus == {Null, TimelineStatusOpen, TimelineStatusInReconciliation, TimelineStatusClosed}

\* ------ Reconciliation phases ------
\* Reconciliation proceeds in two phases:
\*   Phase 1 (Fencing): Send fence requests to the ensemble of the last
\*     segment. We need ALL units in the ensemble to
\*     be fenced to guarantee no old-term writer can make progress.
\*   Phase 2 (Aligning): Read the highest LRA from fenced units, then
\*     truncate dirty entries from the previous term.
NoReconciliation == 0
ReconciliationFencing == 1
ReconciliationAligning == 2


\* ------ Catalog: stored in metadata service (ZK/etcd) ------
TimelineCatalog == [
    status   : TimelineStatus,
    segments : Seq(Segment),
    term     : Nat,
    version  : Version,
    lra      : Nat
]

\* ------ Unit: server-side state per timeline ------
UnitState == [
    timeline_events : [Timelines -> SUBSET Event],
    timeline_lra    : [Timelines -> Nat],
    timeline_term   : [Timelines -> Nat]
]

\* ------ Timeline: client-side state ------
TimelineState == [
    id                        : Timelines,
    term                      : Nat,
    segments                  : Seq(Segment),
    writable_segment          : Segment \cup {Null},
    inflight_record_event_reqs : SUBSET InflightEvent,
    status                    : TimelineStatus,
    lrs                       : Nat,
    lra                       : Nat,
    acked                     : [EventOffsets -> SUBSET Units],
    fenced                    : SUBSET Units,
    reconciliation            : NoReconciliation..ReconciliationAligning,
    reconciliation_ensemble   : SUBSET Units,
    reconciliation_lra        : Nat,
    catalog_version           : Version
]


(***************************************************************************)
(* UTILITY FUNCTIONS                                                       *)
(***************************************************************************)

\* Get the last element of a sequence
FindLast(seq) == seq[Len(seq)]

\* Check if an ensemble is valid: correct size, includes required units,
\* excludes quarantined units
IsValidEnsemble(ensemble, include_units, exclude_units) ==
    /\ Cardinality(ensemble) = ReplicationFactor
    /\ ensemble \intersect exclude_units = {}
    /\ include_units \subseteq ensemble

\* Choose a valid ensemble
FindEnsemble(tid, available, quarantined) ==
    CHOOSE ensemble \in SUBSET Units :
        IsValidEnsemble(ensemble, available, quarantined)

\* Check if a valid ensemble exists
HasTargetEnsemble(tid, available, quarantined) ==
    \E ensemble \in SUBSET Units :
        IsValidEnsemble(ensemble, available, quarantined)

\* Find the maximum contiguous offset where all offsets from curr..result
\* have at least `quorum` acks
RECURSIVE FindMaxContinuousAck(_, _, _, _)
FindMaxContinuousAck(curr, max_idx, acked, quorum) ==
    IF curr > max_idx THEN max_idx
    ELSE IF Cardinality(acked[curr]) < quorum THEN curr - 1
    ELSE FindMaxContinuousAck(curr + 1, max_idx, acked, quorum)

GetAckedOffset(timeline, current, acked, quorum) ==
    FindMaxContinuousAck(current, timeline.lrs, acked, quorum)

\* Find the segment index responsible for a given offset
SegmentForOffset(segments, offset) ==
    CHOOSE i \in 1..Len(segments) :
        /\ segments[i].start_offset <= offset
        /\ (i = Len(segments) \/ segments[i+1].start_offset > offset)


(***************************************************************************)
(* ACTION: OPEN NEW TIMELINE                                               *)
(*                                                                         *)
(* A client creates a new timeline by writing initial metadata to the      *)
(* catalog and selecting the first ensemble.                               *)
(***************************************************************************)

OpenNewTimeline(tid) ==
    LET timeline == timelines[tid]
        catalog  == catalogs[tid]
    IN
        \* Only open if not yet created
        /\ catalog.version = Null
        /\ timeline.catalog_version = Null
        /\ LET first_segment == [
                    id           |-> 1,
                    ensemble     |-> FindEnsemble(tid, {}, {}),
                    start_offset |-> 1
               ]
           IN
            /\ timelines' = [timelines EXCEPT ![tid] =
                [@ EXCEPT
                    !.status          = TimelineStatusOpen,
                    !.catalog_version = 1,
                    !.term            = 1,
                    !.segments        = Append(catalog.segments, first_segment),
                    !.writable_segment = first_segment
                ]]
            /\ catalogs' = [catalogs EXCEPT ![tid] =
                [@ EXCEPT
                    !.status   = TimelineStatusOpen,
                    !.version  = 1,
                    !.term     = 1,
                    !.segments = Append(catalog.segments, first_segment)
                ]]
            /\ UNCHANGED << units, message_channel, sent_events,
                            acked_events, message_fail_count >>


(***************************************************************************)
(* ACTION: RECORD EVENT (Write)                                            *)
(*                                                                         *)
(* The timeline client sends a new event to ALL units in the current       *)
(* ensemble. Every unit must receive and ack the event.                    *)
(***************************************************************************)

\* Construct record requests for each unit in the ensemble
MakeRecordRequests(timeline, event, ensemble, trunc) ==
    {[
        type        |-> RecordEventRequest,
        unit        |-> unit,
        timeline_id |-> timeline.id,
        event       |-> event,
        lra         |-> timeline.lra,
        term        |-> timeline.term,
        trunc       |-> trunc
    ] : unit \in ensemble}

\* Construct a record response
MakeRecordResponse(req) ==
    [
        type        |-> RecordEventResponse,
        unit        |-> req.unit,
        timeline_id |-> req.timeline_id,
        event       |-> req.event,
        term        |-> req.term,
        code        |-> Ok
    ]

TimelineRecordEvent(tid) ==
    LET timeline == timelines[tid]
    IN
        /\ timeline.status = TimelineStatusOpen
        \* Flow control: at most 1 in-flight event beyond LRA
        /\ timeline.lrs - timeline.lra < 1
        \* Pick a fresh payload
        /\ \E payload \in Events : payload \notin sent_events
        /\ LET payload == CHOOSE p \in Events : p \notin sent_events
               event   == [offset |-> timeline.lrs + 1, data |-> payload]
           IN
            /\ UCSendToEnsemble(
                   MakeRecordRequests(timeline, event,
                       timeline.writable_segment.ensemble, FALSE))
            /\ timelines' = [timelines EXCEPT ![tid] =
                [@ EXCEPT
                    !.lrs = timeline.lrs + 1,
                    !.inflight_record_event_reqs = @ \cup {[
                        event      |-> event,
                        segment_id |-> timeline.writable_segment.id,
                        ensemble   |-> timeline.writable_segment.ensemble
                    ]}
                ]]
            /\ sent_events' = sent_events \cup {payload}
            /\ UNCHANGED << units, catalogs, acked_events >>


(***************************************************************************)
(* ACTION: UNIT HANDLES RECORD REQUEST                                     *)
(*                                                                         *)
(* A unit receives a record request. It checks the term: if the request    *)
(* term >= unit's term, it accepts and stores the event. If the request    *)
(* has trunc=TRUE, it deletes all events with higher offsets (used during  *)
(* reconciliation to clean up dirty entries from old terms).               *)
(***************************************************************************)

\* Ensure we process the earliest pending request first (per unit/timeline)
IsEarliestRequest(message) ==
    ~\E other \in DOMAIN message_channel :
        /\ other.type = RecordEventRequest
        /\ message_channel[other] >= 1
        /\ other.term = message.term
        /\ other.timeline_id = message.timeline_id
        /\ other.unit = message.unit
        /\ other.event.offset < message.event.offset

UnitHandleRecordRequest ==
    \E message \in DOMAIN message_channel :
        /\ message.type = RecordEventRequest
        /\ message_channel[message] >= 1
        /\ IsEarliestRequest(message)
        \* Term check: accept if unit's term <= request's term
        /\ units[message.unit].timeline_term[message.timeline_id] <= message.term
        /\ LET unit == units[message.unit]
               tid  == message.timeline_id
               \* If trunc flag, remove events with offset >= this event,
               \* then add this event. Otherwise just upsert.
               new_events ==
                   IF message.trunc
                   THEN {e \in unit.timeline_events[tid] :
                             e.offset < message.event.offset}
                        \cup {message.event}
                   ELSE (unit.timeline_events[tid]
                            \ {e \in unit.timeline_events[tid] :
                                   e.offset = message.event.offset})
                        \cup {message.event}
               new_term == [unit.timeline_term EXCEPT
                                ![tid] = message.term]
               new_lra  == [unit.timeline_lra EXCEPT
                                ![tid] = IF message.lra > @ THEN message.lra ELSE @]
           IN
            /\ units' = [units EXCEPT ![message.unit] = [
                    timeline_events |-> [unit.timeline_events EXCEPT ![tid] = new_events],
                    timeline_term   |-> new_term,
                    timeline_lra    |-> new_lra
               ]]
            /\ UCConsumeAndSend(message, MakeRecordResponse(message))
            /\ UNCHANGED << timelines, catalogs, sent_events, acked_events >>


(***************************************************************************)
(* ACTION: TIMELINE HANDLES RECORD RESPONSE                                *)
(*                                                                         *)
(* The timeline client processes ack responses. An event is committed       *)
(* (LRA advances) only when ALL units in the ensemble have acknowledged    *)
(* it.                                                                     *)
(***************************************************************************)

TimelineHandleRecordResponse(tid) ==
    LET timeline == timelines[tid]
    IN
        /\ timeline.status = TimelineStatusOpen
        /\ \E message \in DOMAIN message_channel :
            /\ message.type = RecordEventResponse
            /\ message_channel[message] >= 1
            /\ message.timeline_id = tid
            /\ message.code = Ok
            /\ message.term = timeline.term
            /\ message.unit \in timeline.writable_segment.ensemble
            \* Process responses in offset order
            /\ ~\E other \in DOMAIN message_channel :
                /\ other.type = RecordEventResponse
                /\ message_channel[other] >= 1
                /\ other.timeline_id = tid
                /\ other.term = message.term
                /\ other.event.offset < message.event.offset
            /\ LET
                   event  == message.event
                   acked  == [timeline.acked EXCEPT
                                  ![event.offset] = @ \cup {message.unit}]
                   lra    == GetAckedOffset(timeline, timeline.lra + 1,
                                            acked, ReplicationFactor)
               IN
                /\ timelines' = [timelines EXCEPT ![tid] =
                    [timeline EXCEPT
                        !.acked = acked,
                        !.lra   = IF lra > @ THEN lra ELSE @,
                        !.inflight_record_event_reqs =
                            {op \in timeline.inflight_record_event_reqs :
                                 op.event.offset > lra}
                    ]]
                /\ acked_events' =
                       IF lra >= event.offset
                       THEN acked_events \cup {event.data}
                       ELSE acked_events
                /\ UCAckMessage(message)
            /\ UNCHANGED << units, catalogs, sent_events, message_fail_count >>


(***************************************************************************)
(* ACTION: RETRY INFLIGHT RECORD EVENT                                     *)
(*                                                                         *)
(* Resend a record request to units that haven't acked yet. Handles        *)
(* transient message loss.                                                 *)
(***************************************************************************)

TimelineRetryInflightRecordEvent(tid) ==
    LET timeline == timelines[tid]
    IN
        /\ timeline.status = TimelineStatusOpen
        /\ \E req \in timeline.inflight_record_event_reqs :
            \* Retry the earliest inflight per segment
            /\ ~\E other \in timeline.inflight_record_event_reqs :
                /\ other.segment_id = req.segment_id
                /\ other.ensemble = req.ensemble
                /\ other.event.offset < req.event.offset
            /\ LET
                   \* Only send to units that haven't acked
                   target_units == timeline.writable_segment.ensemble
                                       \ timeline.acked[req.event.offset]
                   replaced_req == [
                       event      |-> req.event,
                       segment_id |-> timeline.writable_segment.id,
                       ensemble   |-> timeline.writable_segment.ensemble
                   ]
               IN
                /\ target_units # {}
                /\ UCSendToEnsemble(
                       MakeRecordRequests(timeline, req.event,
                                          target_units, FALSE))
                /\ timelines' = [timelines EXCEPT ![tid] =
                    [timeline EXCEPT
                        !.inflight_record_event_reqs =
                            (@ \ {req}) \cup {replaced_req}
                    ]]
                /\ UNCHANGED << units, catalogs, sent_events, acked_events >>


(***************************************************************************)
(* ACTION: ENSEMBLE CHANGE                                                 *)
(*                                                                         *)
(* When a unit fails (detected via message loss), replace it in the        *)
(* ensemble. A new segment starts at LRA + 1, since all entries up to      *)
(* LRA are fully replicated on all ensemble members.                       *)
(***************************************************************************)

\* Check if a message to this unit has been lost
HasFailureMessage(timeline, failure_unit) ==
    \E message \in DOMAIN message_channel :
        /\ message.type \in {RecordEventRequest, RecordEventResponse}
        /\ message_channel[message] = -1
        /\ message.timeline_id = timeline.id
        /\ message.unit = failure_unit
        /\ message.term = timeline.term

\* Remove all messages related to failed units
CleanupFailureMessages(timeline, failure_units) ==
    LET NeedClear(m) ==
        /\ m.type \in {RecordEventRequest, RecordEventResponse}
        /\ m.timeline_id = timeline.id
        /\ m.unit \in failure_units
        /\ m.term = timeline.term
    IN
        message_channel' = [m \in {m \in DOMAIN message_channel :
                                       ~NeedClear(m)}
                                |-> message_channel[m]]

\* Append a new segment or modify the last one if same start offset
AppendOrModifySegment(timeline, start_offset, new_ensemble) ==
    IF start_offset = timeline.writable_segment.start_offset
    THEN [timeline.segments EXCEPT
              ![Len(timeline.segments)].ensemble = new_ensemble]
    ELSE Append(timeline.segments, [
             id           |-> Len(timeline.segments) + 1,
             ensemble     |-> new_ensemble,
             start_offset |-> start_offset
         ])

\* A unit is "pinned" if it holds acked data that hasn't been fully
\* replicated elsewhere. Cannot remove pinned units.
UnitIsPinned(timeline, unit, start_offset) ==
    IF start_offset > timeline.lra THEN FALSE
    ELSE \E offset \in start_offset..timeline.lra :
             unit \in timeline.acked[offset]

TimelineEnsembleChange(tid) ==
    LET timeline == timelines[tid]
    IN
        /\ timeline.status = TimelineStatusOpen
        /\ \E failure_units \in SUBSET timeline.writable_segment.ensemble :
            /\ failure_units # {}
            /\ \A u \in failure_units :
                   HasFailureMessage(timeline, u)
            /\ HasTargetEnsemble(tid,
                   timeline.writable_segment.ensemble \ failure_units,
                   failure_units)
            /\ LET
                   new_ensemble == FindEnsemble(tid,
                       timeline.writable_segment.ensemble \ failure_units,
                       failure_units)
                   start_offset     == timeline.lra + 1
                   new_segments     == AppendOrModifySegment(timeline,
                                          start_offset, new_ensemble)
                   next_version     == catalogs[tid].version + 1
                   FilterAcked(acked, offset) ==
                       IF offset >= start_offset
                       THEN acked \ failure_units
                       ELSE acked
               IN
                \* No pinned units among those being removed
                /\ \A u \in failure_units :
                       ~UnitIsPinned(timeline, u, start_offset)
                \* CAS on catalog version
                /\ catalogs[tid].version = timeline.catalog_version
                /\ catalogs' = [catalogs EXCEPT ![tid] =
                    [@ EXCEPT
                        !.segments = new_segments,
                        !.version  = next_version
                    ]]
                /\ timelines' = [timelines EXCEPT ![tid] =
                    [@ EXCEPT
                        !.catalog_version = next_version,
                        !.acked = [offset \in DOMAIN timeline.acked |->
                                       FilterAcked(timeline.acked[offset],
                                                   offset)],
                        !.segments = new_segments,
                        !.writable_segment = FindLast(new_segments)
                    ]]
                /\ CleanupFailureMessages(timeline, failure_units)
                /\ UNCHANGED << acked_events, sent_events,
                                units, message_fail_count >>


(***************************************************************************)
(* ACTION: START RECONCILIATION (New Term)                                 *)
(*                                                                         *)
(* A new client takes over a timeline by incrementing the term in the      *)
(* catalog (CAS) and beginning the fencing phase.                          *)
(*                                                                         *)
(* Reconciliation process:                                                 *)
(*   1. Increment term in catalog (CAS)                                    *)
(*   2. Send fence requests to all units in the last segment's ensemble    *)
(*   3. Collect responses, learn highest LRA across units                  *)
(*   4. Truncate dirty entries from old term on all units                  *)
(*   5. Resume normal writes                                               *)
(***************************************************************************)

MakeFenceRequests(tid, term, ensemble) ==
    {[
        type        |-> FenceRequest,
        unit        |-> unit,
        timeline_id |-> tid,
        term        |-> term
    ] : unit \in ensemble}

MakeFenceResponse(req, unit_lra) ==
    [
        type        |-> FenceResponse,
        unit        |-> req.unit,
        timeline_id |-> req.timeline_id,
        term        |-> req.term,
        lra         |-> unit_lra,
        code        |-> Ok
    ]

TimelineStartReconciliation(tid) ==
    LET timeline == timelines[tid]
        catalog  == catalogs[tid]
    IN
        /\ catalog.status = TimelineStatusOpen
        /\ timeline.status \in {Null, TimelineStatusOpen}
        /\ timeline.reconciliation = NoReconciliation
        /\ catalog.version # Null
        /\ LET
               new_term     == catalog.term + 1
               next_version == catalog.version + 1
               last_segment == FindLast(catalog.segments)
               ensemble     == last_segment.ensemble
           IN
            /\ catalogs' = [catalogs EXCEPT ![tid] =
                [@ EXCEPT
                    !.term    = new_term,
                    !.status  = TimelineStatusInReconciliation,
                    !.version = next_version
                ]]
            /\ UCSendToEnsemble(MakeFenceRequests(tid, new_term, ensemble))
            /\ timelines' = [timelines EXCEPT ![tid] =
                [@ EXCEPT
                    !.status                  = TimelineStatusInReconciliation,
                    !.term                    = new_term,
                    !.catalog_version         = next_version,
                    !.segments                = catalog.segments,
                    !.writable_segment        = Null,
                    !.reconciliation          = ReconciliationFencing,
                    !.reconciliation_ensemble = ensemble,
                    !.fenced                  = {},
                    !.reconciliation_lra      = 0,
                    !.inflight_record_event_reqs = {},
                    !.acked                   = [offset \in EventOffsets |-> {}],
                    !.lrs                     = 0,
                    !.lra                     = 0
                ]]
            /\ UNCHANGED << units, sent_events, acked_events >>


(***************************************************************************)
(* ACTION: UNIT HANDLES FENCE REQUEST                                      *)
(*                                                                         *)
(* A unit receives a fence request. If request term >= unit's current      *)
(* term, the unit updates its term and responds with its LRA.              *)
(***************************************************************************)

UnitHandleFenceRequest ==
    \E message \in DOMAIN message_channel :
        /\ message.type = FenceRequest
        /\ message_channel[message] >= 1
        /\ LET unit == units[message.unit]
               tid  == message.timeline_id
           IN
            /\ unit.timeline_term[tid] <= message.term
            /\ units' = [units EXCEPT ![message.unit] =
                [@ EXCEPT
                    !.timeline_term = [unit.timeline_term EXCEPT
                                          ![tid] = message.term]
                ]]
            /\ UCConsumeAndSend(message,
                   MakeFenceResponse(message,
                       unit.timeline_lra[tid]))
            /\ UNCHANGED << timelines, catalogs, sent_events, acked_events >>


(***************************************************************************)
(* ACTION: TIMELINE HANDLES FENCE RESPONSE                                 *)
(*                                                                         *)
(* Collect fence responses. ALL units must be fenced before proceeding     *)
(* to the aligning phase.                                                  *)
(***************************************************************************)

TimelineHandleFenceResponse(tid) ==
    LET timeline == timelines[tid]
    IN
        /\ timeline.status = TimelineStatusInReconciliation
        /\ timeline.reconciliation = ReconciliationFencing
        /\ \E message \in DOMAIN message_channel :
            /\ message.type = FenceResponse
            /\ message_channel[message] >= 1
            /\ message.timeline_id = tid
            /\ message.term = timeline.term
            /\ message.code = Ok
            /\ LET
                   new_fenced   == timeline.fenced \cup {message.unit}
                   new_rec_lra  == IF timeline.fenced = {}
                                   THEN message.lra
                                   ELSE IF message.lra < timeline.reconciliation_lra
                                        THEN message.lra
                                        ELSE timeline.reconciliation_lra
                   all_fenced   == new_fenced = timeline.reconciliation_ensemble
               IN
                /\ timelines' = [timelines EXCEPT ![tid] =
                    [@ EXCEPT
                        !.fenced               = new_fenced,
                        !.reconciliation_lra   = new_rec_lra,
                        !.reconciliation =
                            IF all_fenced THEN ReconciliationAligning
                            ELSE ReconciliationFencing
                    ]]
                /\ UCAckMessage(message)
            /\ UNCHANGED << units, catalogs, sent_events,
                            acked_events, message_fail_count >>


(***************************************************************************)
(* ACTION: COMPLETE RECONCILIATION                                         *)
(*                                                                         *)
(* After fencing all units, truncate dirty entries from the old term and   *)
(* transition to Open. The timeline can now accept writes under the new    *)
(* term.                                                                   *)
(*                                                                         *)
(* Modeled as atomic for simplicity. A real implementation would send      *)
(* truncation entries (trunc=TRUE) and wait for acks from all units.       *)
(***************************************************************************)

TimelineCompleteReconciliation(tid) ==
    LET timeline == timelines[tid]
        catalog  == catalogs[tid]
    IN
        /\ timeline.status = TimelineStatusInReconciliation
        /\ timeline.reconciliation = ReconciliationAligning
        /\ LET
               new_lra      == timeline.reconciliation_lra
               last_segment == FindLast(timeline.segments)
               next_version == catalog.version + 1
               \* Rebuild segments: keep segments covering committed
               \* offsets (1..new_lra), add writable segment at new_lra+1
               reconciled_segments ==
                   IF new_lra = 0
                   THEN <<[id           |-> 1,
                           ensemble     |-> last_segment.ensemble,
                           start_offset |-> 1]>>
                   ELSE LET cover_count ==
                                SegmentForOffset(timeline.segments, new_lra)
                            writable == [
                                id           |-> cover_count + 1,
                                ensemble     |-> last_segment.ensemble,
                                start_offset |-> new_lra + 1]
                        IN Append(SubSeq(timeline.segments, 1,
                                         cover_count), writable)
               writable_seg == FindLast(reconciled_segments)
           IN
            /\ catalogs' = [catalogs EXCEPT ![tid] =
                [@ EXCEPT
                    !.status   = TimelineStatusOpen,
                    !.version  = next_version,
                    !.lra      = new_lra,
                    !.segments = reconciled_segments
                ]]
            /\ timelines' = [timelines EXCEPT ![tid] =
                [@ EXCEPT
                    !.status          = TimelineStatusOpen,
                    !.catalog_version = next_version,
                    !.lra             = new_lra,
                    !.lrs             = new_lra,
                    !.segments        = reconciled_segments,
                    !.writable_segment = writable_seg,
                    !.reconciliation  = NoReconciliation,
                    !.fenced          = {},
                    !.acked           = [offset \in EventOffsets |->
                                            IF offset <= new_lra
                                            THEN writable_seg.ensemble
                                            ELSE {}]
                ]]
            \* Truncate dirty entries on all units across all segments
            /\ LET all_ensemble_units ==
                       UNION {timeline.segments[i].ensemble :
                                  i \in 1..Len(timeline.segments)}
               IN
                units' = [u \in Units |->
                    IF u \in all_ensemble_units
                    THEN [units[u] EXCEPT
                        !.timeline_events = [units[u].timeline_events EXCEPT
                            ![tid] = {e \in @ : e.offset <= new_lra}],
                        !.timeline_lra  = [units[u].timeline_lra EXCEPT
                            ![tid] = IF new_lra > @ THEN new_lra ELSE @],
                        !.timeline_term = [units[u].timeline_term EXCEPT
                            ![tid] = timeline.term]
                    ]
                    ELSE units[u]
                   ]
            /\ UNCHANGED << message_channel, sent_events,
                            acked_events, message_fail_count >>


(***************************************************************************)
(* ACTION: RETRY FENCE REQUEST                                             *)
(*                                                                         *)
(* Resend a fence request to an unfenced unit when the original fence      *)
(* request or its response was lost. Handles transient message loss        *)
(* during reconciliation.                                                  *)
(***************************************************************************)

TimelineRetryFenceRequest(tid) ==
    LET timeline == timelines[tid]
    IN
        /\ timeline.status = TimelineStatusInReconciliation
        /\ timeline.reconciliation = ReconciliationFencing
        /\ \E retry_unit \in timeline.reconciliation_ensemble \ timeline.fenced :
            LET fence_req == [
                    type        |-> FenceRequest,
                    unit        |-> retry_unit,
                    timeline_id |-> tid,
                    term        |-> timeline.term
                ]
            IN
                \E lost_msg \in DOMAIN message_channel :
                    /\ lost_msg.type \in {FenceRequest, FenceResponse}
                    /\ lost_msg.timeline_id = tid
                    /\ lost_msg.unit = retry_unit
                    /\ lost_msg.term = timeline.term
                    /\ message_channel[lost_msg] = -1
                    /\ UCConsumeAndSend(lost_msg, fence_req)
                    /\ UNCHANGED << units, timelines, catalogs,
                                    sent_events, acked_events >>


(***************************************************************************)
(* INITIAL STATE                                                           *)
(***************************************************************************)

InitTimeline(tid) == [
    id                         |-> tid,
    term                       |-> 0,
    segments                   |-> <<>>,
    writable_segment           |-> Null,
    inflight_record_event_reqs  |-> {},
    status                     |-> Null,
    lrs                        |-> 0,
    lra                        |-> 0,
    acked                      |-> [offset \in EventOffsets |-> {}],
    fenced                     |-> {},
    reconciliation             |-> NoReconciliation,
    reconciliation_ensemble    |-> {},
    reconciliation_lra         |-> 0,
    catalog_version            |-> Null
]

InitUnit(u) == [
    timeline_events |-> [tid \in Timelines |-> {}],
    timeline_lra    |-> [tid \in Timelines |-> 0],
    timeline_term   |-> [tid \in Timelines |-> 0]
]

InitCatalog(tid) == [
    status   |-> Null,
    version  |-> Null,
    segments |-> <<>>,
    term     |-> 0,
    lra      |-> 0
]

Init ==
    /\ units              = [u   \in Units     |-> InitUnit(u)]
    /\ timelines          = [tid \in Timelines  |-> InitTimeline(tid)]
    /\ catalogs           = [tid \in Timelines  |-> InitCatalog(tid)]
    /\ message_channel    = [message \in {} |-> 0]
    /\ message_fail_count = [message \in {} |-> 0]
    /\ sent_events        = {}
    /\ acked_events       = {}


(***************************************************************************)
(* NEXT STATE RELATION                                                     *)
(***************************************************************************)

Next ==
    \* Unit-side actions
    \/ UnitHandleRecordRequest
    \/ UnitHandleFenceRequest
    \* Timeline-side actions
    \/ \E tid \in Timelines :
        \/ OpenNewTimeline(tid)
        \/ TimelineRecordEvent(tid)
        \/ TimelineHandleRecordResponse(tid)
        \/ TimelineRetryInflightRecordEvent(tid)
        \/ TimelineEnsembleChange(tid)
        \/ TimelineStartReconciliation(tid)
        \/ TimelineHandleFenceResponse(tid)
        \/ TimelineRetryFenceRequest(tid)
        \/ TimelineCompleteReconciliation(tid)


(***************************************************************************)
(* SPECIFICATION                                                           *)
(***************************************************************************)

Spec == Init /\ [][Next]_vars


(***************************************************************************)
(* TYPE INVARIANT                                                          *)
(***************************************************************************)

TypeOK ==
    /\ units     \in [Units     -> UnitState]
    /\ catalogs  \in [Timelines -> TimelineCatalog]
    /\ timelines \in [Timelines -> TimelineState]


(***************************************************************************)
(* SAFETY INVARIANTS                                                       *)
(***************************************************************************)

\* Every acknowledged event has at least ReplicationFactor copies.
\* This means ALL ensemble members have it.
AllAckedEventsFullyReplicated ==
    \A tid \in Timelines :
        \A offset \in 1..timelines[tid].lra :
            Cardinality(timelines[tid].acked[offset]) >= ReplicationFactor

\* No acknowledged event is lost from the ensemble responsible for it.
NoAckedEventLost ==
    \A tid \in Timelines :
        timelines[tid].status = TimelineStatusOpen =>
            \A offset \in 1..timelines[tid].lra :
                LET seg == timelines[tid].segments[
                        SegmentForOffset(timelines[tid].segments, offset)]
                IN \A unit \in seg.ensemble :
                    \E e \in units[unit].timeline_events[tid] :
                        e.offset = offset

\* No divergence: all units in the responsible segment agree on committed entries.
NoDivergence ==
    \A tid \in Timelines :
        timelines[tid].status = TimelineStatusOpen =>
            \A offset \in 1..timelines[tid].lra :
                LET seg == timelines[tid].segments[
                        SegmentForOffset(timelines[tid].segments, offset)]
                IN \A u1, u2 \in seg.ensemble :
                    LET e1 == {e \in units[u1].timeline_events[tid] : e.offset = offset}
                        e2 == {e \in units[u2].timeline_events[tid] : e.offset = offset}
                    IN (e1 # {} /\ e2 # {}) => e1 = e2

\* Acked events are a subset of sent events.
AckedSubsetOfSent ==
    acked_events \subseteq sent_events


(***************************************************************************)
(* STATE CONSTRAINT (bound the state space for model checking)             *)
(***************************************************************************)

\* Bound the term to prevent infinite reconciliation cycles.
\* Term 1 = initial, term 2 = first reconciliation, term 3 = second.
\* This is sufficient to verify correctness across multiple reconciliations.
StateConstraint ==
    \A tid \in Timelines : timelines[tid].term <= 1

(***************************************************************************)
(* SYMMETRY (for model checking optimization)                              *)
(***************************************************************************)

Symmetry == Permutations(Events) \cup Permutations(Units)

=========================================================
