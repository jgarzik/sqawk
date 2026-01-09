# Sqawk SQL Feature Expansion TODO

## Mission
Support all SQL commands from sqlparser crate that make sense for CSV file collections, implementing VM operations similar to SQLite opcodes.

## Strategy
**VM-only for new features** - New SQL features implemented in VM execution path only.

---

## Phase 1: Expression Enhancements
- [x] LIKE/ILIKE pattern matching (opcodes: Like, Glob)
- [x] BETWEEN operator (opcodes: reuse Ge/Le with conditional jumps)
- [x] IN (list) operator (opcodes: reuse Eq with conditional jumps)
- [x] CASE WHEN expressions (opcodes: reuse Eq/comparisons with IfZ/Goto jumps)
- [x] CAST type conversion (opcodes: Cast)
- [x] COALESCE/NULLIF (opcodes: IsNull with conditional jumps)
- [x] **Phase 1 Validation**: build, test, clippy clean

---

## Phase 2: Set Operations
- [x] UNION / UNION ALL (opcodes: Distinct for dedup)
- [x] INTERSECT (opcodes: Intersect)
- [x] EXCEPT (opcodes: Except)
- [x] **Phase 2 Validation**: build, test, clippy clean

---

## Phase 3: Outer Joins
- [x] LEFT OUTER JOIN (opcodes: NullRow, RewindInner, MarkMatch, nested loop join)
- [x] RIGHT OUTER JOIN (opcodes: similar to LEFT, swap tables)
- [x] FULL OUTER JOIN (opcodes: combines LEFT and RIGHT logic)
- [x] **Phase 3 Validation**: build, test, clippy clean

---

## Phase 4: Subqueries (Non-Correlated)
- [x] Scalar subqueries (compile-time execution with aggregates)
- [x] IN (subquery) (compile-time value collection)
- [x] EXISTS operator (compile-time row check)
- [x] **Phase 4 Validation**: build, test, clippy clean

---

## Phase 4B: Correlated Subqueries
- [x] Correlated scalar subqueries (nested loop with Null accumulator init, AggStep/AggFinal)
- [x] Correlated EXISTS (nested loop with early exit)
- [x] Correlated NOT EXISTS (nested loop with negation)
- [x] **Phase 4B Validation**: build, test, clippy clean

---

## Phase 5: VM Completeness for DML
- [x] VM INSERT (opcodes: OpenWrite, InsertRow)
- [x] VM UPDATE (opcodes: Column, DeleteRow, InsertRow)
- [x] VM DELETE (opcodes: OpenRead, DeleteRow)
- [x] INSERT...SELECT (opcodes: OpenRead, OpenWrite, Column, InsertRow)
- [x] **Phase 5 Validation**: build, test, clippy clean

---

## Phase 6: VM Completeness for Aggregates & Sorting
- [x] VM COUNT/SUM/AVG/MIN/MAX (opcodes: AggStep, AggFinal) - 5 aggregate tests
- [x] VM GROUP BY (compile-time grouping) - 5 GROUP BY tests
- [x] VM ORDER BY (opcodes: SorterOpen, SorterInsert, SorterSort) - 8 ORDER BY tests
- [x] VM DISTINCT (opcode: Distinct) - 7 DISTINCT tests
- [x] **Phase 6 Validation**: build, test, clippy clean (all 186 tests pass)

---

## Phase 7: Advanced Expressions
- [x] Math functions: ABS, ROUND, CEIL, FLOOR (opcodes: MathFunc)
- [x] String functions: LENGTH, CONCAT, LEFT, RIGHT (opcodes: StringFunc)
- [x] Date functions: DATE, TIME, NOW (opcodes: DateFunc)
- [x] Arithmetic operators: +, -, *, /, % (opcodes: Add, Subtract, Multiply, Divide, Remainder)
- [x] **Phase 7 Validation**: build, test, clippy clean (186 tests pass)

---

## Phase 8: Window Functions
- [ ] ROW_NUMBER() (opcodes: WindowStep, WindowValue)
- [ ] RANK(), DENSE_RANK() (opcodes: WindowStep, WindowValue)
- [ ] LAG(), LEAD() (opcodes: WindowStep, WindowValue)
- [ ] SUM/AVG/COUNT OVER() (opcodes: AggStep with window frame)
- [ ] **Phase 8 Validation**: build, test, clippy clean

---

## Phase 9: Common Table Expressions (CTEs)
- [ ] Non-recursive CTEs (opcodes: CteOpen, CteRead)
- [ ] Multiple CTEs in single query
- [ ] Recursive CTEs (optional) (opcodes: RecursiveCte)
- [ ] **Phase 9 Validation**: build, test, clippy clean

---

## Phase 10: DDL Enhancements
- [ ] DROP TABLE (opcodes: DropTable, Destroy)
- [ ] ALTER TABLE ADD COLUMN (opcodes: AlterTable)
- [ ] CREATE TABLE AS SELECT (opcodes: CreateTableAs)
- [ ] TRUNCATE TABLE (opcodes: Clear)
- [ ] **Phase 10 Validation**: build, test, clippy clean

---

## Test Files to Create
- `tests/like/mod.rs`
- `tests/between/mod.rs`
- `tests/in_list/mod.rs`
- `tests/case/mod.rs`
- `tests/cast/mod.rs`
- `tests/coalesce/mod.rs`
- `tests/union/mod.rs`
- `tests/intersect/mod.rs`
- `tests/except/mod.rs`
- `tests/left_join/mod.rs`
- `tests/right_join/mod.rs`
- `tests/full_join/mod.rs`
- `tests/subquery/mod.rs`
- `tests/in_subquery/mod.rs`
- `tests/exists/mod.rs`
- `tests/correlated/mod.rs`
- `tests/vm/insert.rs`
- `tests/vm/update.rs`
- `tests/vm/delete.rs`
- `tests/insert_select/mod.rs`
- `tests/vm/aggregates.rs`
- `tests/vm/group_by.rs`
- `tests/vm/order_by.rs`
- `tests/vm/distinct.rs`
- `tests/math_functions/mod.rs`
- `tests/string_functions_ext/mod.rs`
- `tests/date_functions/mod.rs`
- `tests/arithmetic/mod.rs`
- `tests/window/mod.rs`
- `tests/cte/mod.rs`
- `tests/drop_table/mod.rs`
- `tests/alter_table/mod.rs`
- `tests/create_as/mod.rs`
- `tests/truncate/mod.rs`
