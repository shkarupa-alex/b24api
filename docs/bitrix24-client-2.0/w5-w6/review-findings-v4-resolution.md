# W5/W6 v4 handoff finding resolution

Rejected packet subject:
`d09158760e0054716a87942015d2afe79e465f86`.

Its production-code subject:
`fd1fb727c4a13d748539948188a8375e8bbf80aa`.

Replacement code-and-test subject:
`a29b58f3faba9a71202bbe1e9a4aab0f770b369b`.

## Finding

| Handoff finding | Resolution in `a29b58f...` | Regression evidence |
|---|---|---|
| Packet v4 claimed committed plan/policy COMPOSITE refusal before reference-source pull, but its committed parameterized test exercised only `iter_list`; `iter_references` had been checked only by a scratch probe. | A dedicated parameterized `iter_references` regression now covers both plan-level and consistency-policy-level `IdentityRequirement.COMPOSITE`. It supplies an async source whose first body action marks it pulled. | Both cases raise typed `CapabilityError`, preserve final report identity, freeze FAILED, leave `source_pulled=false`, and perform zero transport calls. The exact strict node expands to 2/2 cases. |

No production implementation changed between `fd1fb727...` and `a29b58f...`.
The rejected packet's behavior claim was correct, but its immutable evidence did
not meet the handoff bar. Packet v5 uses the new code-and-test SHA and includes
the reference-source node in both targeted and consolidated strict gates.
