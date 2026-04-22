# CypherGlot Paper Plan

## Goal

Turn CypherGlot into a database-systems paper about backend-aware Cypher-to-SQL
compilation over relational engines, with reproducible compiler and runtime
evaluation.

This should be framed as a research paper, not an industry paper.

## Target Venues

Primary targets, in order of fit:

1. SIGMOD research track
2. VLDB / PVLDB research track
3. ICDE research track

Why this venue family fits:

- graph data management
- query languages and compilation
- query processing and optimization
- backend-aware system design
- benchmarking and reproducibility

## Recommended Track Positioning

Current best positioning:

- SIGMOD: Regular Research Paper
- VLDB: Systems Paper
- ICDE: Regular Research Paper

Backup positioning if the empirical story becomes stronger than the system
novelty story:

- SIGMOD: Experiments & Analysis
- VLDB: Experiment, Analysis & Benchmark (EA&B)
- ICDE: Experimental, Analysis, and Benchmark (EAB)

Default plan: write this as a systems paper first, with the benchmark/evaluation
story as supporting evidence rather than the only contribution.

## Core Paper Claim

CypherGlot shows that a practical admitted subset of Cypher can be compiled into
portable, backend-aware SQL programs across multiple relational engines, with a
compiler architecture that separates frontend normalization from backend-aware
lowering and with empirical evidence about where performance costs actually come
from.

Short version:

- practical Cypher-to-SQL compilation
- one frontend, multiple SQL backends
- graph workloads over relational engines
- careful separation of compile, render, and runtime costs

## Candidate Title Directions

Working title directions only:

1. CypherGlot: Backend-Aware Compilation of Cypher to Relational SQL
2. Compiling Cypher to Portable SQL Across Relational Backends
3. Backend-Aware Cypher-to-SQL Compilation for Graph Workloads on Relational Engines

Pick a title later based on the final emphasis.

## Intended Contribution Story

The paper should argue for these contributions:

1. A compiler architecture for translating an admitted Cypher subset into
   backend-aware SQLGlot IR and rendered SQL/program text.
2. A portability layer that targets multiple relational backends with shared
   frontend processing and backend-specific lowering.
3. A reproducible benchmark methodology that separates parser/validator/
   normalizer/compiler costs from runtime execution costs.
4. An empirical study of backend behavior across SQLite, DuckDB, and PostgreSQL,
   including what is shared across backends and what remains dialect- or
   engine-specific.

The paper should not be sold as only a syntax translator. The stronger story is
compiler architecture plus backend portability plus careful evidence.

## What Makes The Paper Interesting

The interesting part is not simply that Cypher can be lowered to SQL.

The interesting part is:

- a clean admitted-subset contract
- one normalized frontend feeding multiple SQL backends
- explicit backend-aware lowering rather than pretending all SQL backends are the same
- empirical separation between compiler and runtime costs
- lessons from real backend behavior, including the earlier DuckDB render issue

## Paper Outline

### 1. Introduction

- problem: graph query interfaces are useful, but graph engines and relational
  engines have fragmented deployment and tooling tradeoffs
- hypothesis: an admitted Cypher subset can be compiled into portable,
  backend-aware SQL programs over mainstream relational systems
- contribution summary
- headline results

### 2. Problem Setting And Scope

- admitted Cypher subset
- supported read and write shapes
- explicit non-goals
- type-aware schema contract
- why backend-aware lowering is necessary

### 3. System Overview

- frontend pipeline: parse, validate, normalize
- IR build and backend binding
- backend-aware lowerers
- rendering path and SQL program emission
- runtime execution model for program-shaped outputs

### 4. Compilation Design

- normalized representation
- shared graph-relational IR
- lowering decisions for reads, writes, variable-length paths, loops, and unwind
- schema assumptions and backend capabilities
- why SQLGlot is used and where CypherGlot’s own logic begins and ends

### 5. Backend-Specific Design

- SQLite
- DuckDB
- PostgreSQL
- common lowering vs dialect-specific rendering
- examples of backend differences that matter

### 6. Experimental Methodology

- compiler benchmark
- runtime benchmark
- schema-shape benchmark if included
- hardware/software setup
- corpora and query families
- metrics: p50/p95/p99, setup costs, RSS checkpoints, runtime categories
- reproducibility and artifact plan

### 7. Results

- compiler latency by entrypoint
- backend-lowering costs
- runtime latency by backend and workload class
- memory behavior and benchmark methodology lessons
- explanation of where backend gaps come from

### 8. Discussion

- portability vs completeness tradeoff
- admitted subset vs full Cypher
- when relational backends are a good fit for graph-style workloads
- limits of current backend support

### 9. Related Work

- graph query languages
- graph-to-relational execution
- graph databases vs relational systems
- query compilation frameworks
- benchmark/reproducibility work in DB systems

### 10. Conclusion

- what CypherGlot enables
- what the experiments show
- where the current limits remain

## Evidence We Already Have

- public compiler entrypoints with backend-aware output
- backend-specific lowering for SQLite, DuckDB, and PostgreSQL
- compiler benchmark with shared and backend-dependent entrypoints
- runtime benchmark harness across multiple backends
- benchmark docs and checked-in result artifacts
- evidence that the old DuckDB compiler-side render slowdown was largely fixed
  by the render-path work and SQLGlot `30.6.0`
- evidence that earlier runtime RSS reporting was distorted by benchmark harness
  bookkeeping, not only by backend engine behavior

## Evidence Still Needed Before Submission

### Must have

1. A stable, fresh runtime baseline after the render-path and RSS-accounting fixes.
2. Clean result selection for the paper, rather than raw benchmark dump style.
3. A concise set of representative queries and workloads for the main paper.
4. A clear statement of the admitted Cypher subset and unsupported constructs.
5. A reproducibility package plan for code, scripts, benchmark corpora, and run commands.

### Very likely needed

1. A stronger comparison section against alternatives or neighboring systems.
2. A short ablation or design justification for major compiler architecture choices.
3. A compact end-to-end case study showing one query family across multiple backends.
4. Clear threat-to-validity language around benchmark environment, subset scope,
   and backend configuration.

### Nice to have

1. Additional medium-scale runtime runs once the harness and claims stabilize.
2. Artifact automation for one-command reproduction of the headline figures.
3. A demo-ready script and visualizations in case a demo submission becomes useful.

## Likely Reviewer Questions

We should be ready to answer these:

1. Why does this need to exist if graph databases already exist?
2. Why is the admitted subset the right subset?
3. What is genuinely novel beyond a translation layer on top of SQLGlot?
4. Which parts are CypherGlot contributions versus SQLGlot contributions?
5. How portable are the generated programs really?
6. How much performance do we lose relative to native alternatives?
7. Are the benchmark claims robust and reproducible?
8. What workloads benefit from this approach, and what workloads do not?

## Risks

Main paper risks:

- contribution looks incremental if the novelty claim is stated too weakly
- paper reads like a library description instead of a systems paper
- evaluation becomes too broad and unfocused
- benchmark story dominates without enough architectural depth
- subset limitations feel too restrictive unless motivated clearly

Mitigation:

- keep the core claim on compiler architecture plus backend-aware portability
- choose a small number of strong experiments rather than many weak ones
- make limitations explicit instead of hiding them
- distinguish CypherGlot logic from SQLGlot dependency behavior carefully

## Immediate Work Plan

### Phase 1: Lock the story

1. Decide the main submission target: SIGMOD first, VLDB second, ICDE third.
2. Freeze the paper claim in two to three sentences.
3. Decide whether the paper is primarily systems or benchmark/evaluation driven.

### Phase 2: Lock the evidence

1. Re-run the post-fix runtime baselines that matter for the main claims.
2. Select the final headline tables and figures.
3. Remove stale benchmark claims from any draft text.

### Phase 3: Lock the artifact

1. Make the benchmark commands and corpora reproducible.
2. Prepare anonymous artifact packaging instructions.
3. Verify that all reported figures can be regenerated from committed scripts.

### Phase 4: Write the paper

1. Draft introduction and contribution list.
2. Draft system design and compilation sections.
3. Draft evaluation section from frozen result tables.
4. Draft limitations, related work, and conclusion.

## Near-Term Target Dates

Given the current date, the realistic targets are:

- VLDB monthly submission cycle: earliest fast-moving option
- ICDE June 11, 2026: realistic if the paper gets stable quickly
- SIGMOD July 17, 2026: likely the most comfortable serious target

Default recommendation:

1. Aim first at SIGMOD research track.
2. Keep VLDB as the flexible rolling alternative.
3. Use ICDE if the paper timing fits that deadline better than SIGMOD.

## Decision For Now

Current default paper plan:

- venue family: database conferences
- primary target: SIGMOD research track
- fallback targets: VLDB, then ICDE
- paper type: systems paper / regular research paper
- backup paper type: experiments and analysis paper if the empirical study
  becomes the strongest contribution

That is the plan until the runtime evidence and final contribution framing are
locked.
