---
name: rxjs-pipes-refactoring
description: Use when refactoring RxJS pipes on the backend — eliminates nested pipes, enforces flat operator chains, state objects, switchMap preference over concatMap, forkJoin with objects, early array expansion, and descriptive parameter names.
---

# RxJS Pipes Refactoring (Backend)

**Declare at the start of work:** "I am using the rxjs-pipes-refactoring skill."

Refactor RxJS pipes for improved readability, maintainability, and performance. Follow functional and reactive programming best practices to create clean, modular, and easily testable data streams. Avoid nested pipes, use state objects for intermediate data, break complex streams into smaller functions, and minimize side effects.

---

## Rules

### 1. Nested `.pipe()` inside operator callbacks — FORBIDDEN WITHOUT EXCEPTION

A nested `.pipe()` occurs when `.someObservable.pipe(...)` appears inside an operator callback. This is ALWAYS a design error.

After every refactoring step **perform a double-check**: find all `.pipe(` occurrences in the file and confirm each one is a top-level stream call (e.g. `context.req.request({...}).pipe(...)` or `return someObs$.pipe(...)`), NOT inside an operator callback.

**Allowed patterns instead of nested pipes:**
- `forkJoin` — for parallel streams
- `state` object + flat operators — for sequential processing
- `tap` for state initialization + `switchMap(item => singleObs)` — state init in a preceding `tap`, `switchMap` returns the observable only. **Prefer this split whenever possible.** When separating is not feasible, `switchMap(item => { state.init(item); return singleObs; })` is acceptable as a fallback — ONE observable returned WITHOUT a `.pipe()` chain.

**ALWAYS PREFER `switchMap` OVER `concatMap` WHEN THERE IS NO DIFFERENCE.**

```typescript
// BAD — nested pipe inside an operator callback
mergeMap(face => {
  return from(encrypt(face.url)).pipe(   // ← nested pipe!
    switchMap(link => insertRecord(link)),
    tap(({ id }) => state.photoId = id),
  );
}),

// GOOD — tap initializes state, switchMap returns ONE observable without .pipe()
tap(face => Object.assign(faceState, { face, photoId: null })),
switchMap(face => from(encrypt(face.url))),
switchMap(link => insertRecord(link)),
tap(({ id }) => Object.assign(faceState, { photoId: id })),
switchMap(() => updateRecord(faceState.face?.id, faceState.photoId)),
```

### 2. Avoid the `iif` operator

Instead of `iif`, use ternary operators. Use `if...else` only when branches are complex; use `switch...case` for many branches.

### 3. Store state outside the stream

Use a `state` object for intermediate data when necessary.

**All `state` mutations happen ONLY inside `tap`.** `switchMap`/`mergeMap` and other operators handle logic and return streams — not state mutation. Update state via `tap` and `Object.assign`.

```typescript
// BAD — state mutation inside switchMap
switchMap(item => {
  state.item = item;
  state.params = buildParams(item);
  return processItem$(state.params);
}),

// GOOD — switchMap for logic only, tap for mutations
switchMap(items => of(items[0])),
tap(item => Object.assign(state, { item, params: buildParams(item) })),
switchMap(() => processItem$(state.params)),
```

### 4. Break complex pipes into smaller functions

Extract into separate functions only complex code blocks or functions with complex object arguments that span multiple lines.

Functions that return observables must end with `$`.

Do not immediately extract everything into separate functions as a way to avoid nested pipes — do this only as a last resort.

### 5. Use RxJS operators for array processing

Instead of `map`, `reduce`, `filter`, and other array methods inside callbacks, prefer RxJS operators.

The key pattern is **"early array expansion"**: use `switchMap(items => from(items))` directly in the main stream, then process individual emissions through flat operators like `filter`, `map`, `mergeMap`, etc. This eliminates both array methods inside `map` and nested `from(items).pipe(...)` at once.

```typescript
// BAD — array methods inside map + nested from().pipe()
map(({ items }: { items: Person[] }) => items.filter(person => person.imageSfsId)),
switchMap(persons => from(persons).pipe(
  mergeMap(person => context.insertRecords({ ... })),
  toArray(),
)),

// GOOD — expand the array immediately, no nested pipe
switchMap(({ items }: { items: Person[] }) => from(items)),
filter(person => !!person.imageSfsId),
mergeMap(person => context.insertRecords({ ... })),
toArray(),
```

The same pattern works with a transforming `map` (items → another type):

```typescript
// BAD
map(({ items }: { items: Row[] }) => {
  return items
    .map(row => firstUuid ? { id: row.id, fileUuid: firstUuid } : null)
    .filter((row): row is { id: string; fileUuid: string } => row !== null);
}),
switchMap(rows => from(rows).pipe(
  mergeMap(row => context.processRow(row)),
  toArray(),
)),

// GOOD
switchMap(({ items }: { items: Row[] }) => from(items)),
map(row => {
  const firstUuid = Array.isArray(row.markFileUUID) ? row.markFileUUID[0] : null;
  return firstUuid ? { id: row.id, fileUuid: firstUuid } : null;
}),
filter((row): row is { id: string; fileUuid: string } => row !== null),
mergeMap(row => context.processRow(row)),
toArray(),
```

### 6. Avoid `subscribe` inside other streams

Do not use `subscribe` inside streams.

### 7. Minimize side effects inside the pipe

Use `tap` for side effects such as logging or state updates.

### 8. Prefer `switchMap` over `mergeMap` and `concatMap`

Use `switchMap` when you do not need concurrency or strict sequential execution. **Always prefer `switchMap` over `concatMap` when there is no behavioral difference.**

### 9. Use `catchError` for error handling

Use `catchError` at the top level of the pipe for global error handling, and between operators when intermediate errors need to be caught and logged.

### 10. Add comments to non-trivial pipes

Explain business logic in comments. Describe algorithm steps above each operator.

### 11. Use `EMPTY` with `defaultIfEmpty`

Use `EMPTY` to skip downstream operators when a condition is not met. `defaultIfEmpty` sets a return value if the stream completed without emitting. `toArray` returns `[]` as a default.

### 12. Use `finalize` for resource cleanup

Use `finalize` for resource cleanup when the stream completes, when necessary.

### 13. Identify race conditions in concurrent logic

Identify possible race conditions, especially in `mergeMap` when mutating state or making network requests. Leave comments or use `concatMap` for sequential execution when ordering matters.

If a race condition arises from mutating shared state, consider whether `Map` and/or `Set` can store the needed information instead.

### 14. Use `of(...)` with a default value at the start of the pipe

For improved readability, begin the pipe with `of(...)`. For example, `of(true)`.

### 15. `forkJoin` is used ONLY with an object, not an array

Pass a named object to `forkJoin` — this makes result destructuring readable and resilient to query reordering.

For concurrent array processing, use `from(...).pipe(mergeMap(...), toArray())`.

```typescript
// BAD — array, positional dependency, fragile destructuring
forkJoin([
  this.req.request({ ... }),
  this.req.request({ ... }),
]).pipe(
  map(([result1, result2]) => ({ ... })),
);

// GOOD — object, named keys, order does not matter
forkJoin({
  result1: this.req.request({ ... }),
  result2: this.req.request({ ... }),
}).pipe(
  map(({ result1, result2 }) => ({ ... })),
);

// GOOD — concurrent array processing via from + mergeMap + toArray
from(items).pipe(
  mergeMap(item => this.processItem$(item)),
  toArray(),
);
```

### 16. "Early array expansion" instead of `switchMap(items => from(items).pipe(...))`

If the source emits once (HTTP response, single request) and each element needs processing without intermediate aggregation — expand the array at the start: `switchMap(items => from(items))`, then process individual emissions with flat operators.

This makes the pipe fully linear and eliminates `from(items).pipe(...)` as a nested construct.

**Applicability**: upstream emits once; elements are processed independently; the full array is not needed inside the callback.

```typescript
// BAD — nested from().pipe() inside switchMap
switchMap(items => from(items).pipe(
  filter(item => item.active),
  mergeMap(item => this.process$(item)),
  toArray(),
)),

// GOOD — linear stream without nested pipe
switchMap(items => from(items)),
filter(item => item.active),
mergeMap(item => this.process$(item)),
toArray(),
```

### 17. Use descriptive parameter names and explicit typing

Callback parameters in operators (`filter`, `map`, `tap`, `switchMap`, etc.) must have **descriptive names** that reveal the nature of the data. Single-letter and abbreviation names (`s`, `f`, `r`, `x`, `el`, `v`) are not allowed.

If the parameter type is not obvious from context, add an **explicit type annotation**.

```typescript
// BAD — unclear what s, f, r are
filter(s => !!s.imageUuid),
switchMap(f => from(new Aes256Cbc().encrypt(f.eventImageUUID))),
mergeMap(r => context.insertRecords({ fields: { link: r } })),

// GOOD — descriptive names, explicit typing where type is not obvious
filter((silhouette: { id: string; imageUuid: string; cropImageUuid: string }) => !!silhouette.imageUuid),
switchMap(() => from(new Aes256Cbc().encrypt(silhouette.imageUuid))), // closure — no redundant parameter needed
mergeMap(encryptedLink => context.insertRecords({ fields: { link: encryptedLink } })),
```

When a value is already accessible through a closure, do not rename it in `switchMap`/`tap` — use `() =>` and reference the variable from the outer scope:

```typescript
// BAD — f duplicates face from closure
concatMap(face => of(face).pipe(
  switchMap(f => from(new Aes256Cbc().encrypt(f.eventImageUUID))),
)),

// GOOD — face from closure, switchMap ignores incoming value via () =>
concatMap(face => of(face).pipe(
  switchMap(() => from(new Aes256Cbc().encrypt(face.eventImageUUID))),
)),
```

### 18. Name state outside the stream for per-item async processing

If `mergeMap`/`concatMap` contains `const state = { ... }` inside the callback — refactor: move state OUTSIDE the `.pipe()` call and give it a descriptive name that includes the stream name (`recognizedFacesPipeState`, `detectedSilhouettesPipeState`, etc.).

Use `concatMap` instead of `mergeMap` here: it guarantees sequential processing and eliminates race conditions when mutating shared state.

Start the per-item chain with `of(item)` (rule 14), referencing `item` via closure in all subsequent operators.

```typescript
// BAD — anonymous state inside callback, mergeMap (possible race conditions)
mergeMap(face => {
  const state = { photoLinkId: null, cropLinkId: null };
  return from(new Aes256Cbc().encrypt(face.eventImageUUID)).pipe(
    switchMap(encryptedLink => context.insertRecords({ fields: { link: encryptedLink } })),
    tap(({ id }) => Object.assign(state, { photoLinkId: id })),
    switchMap(() => context.updateRecords({ fields: { photoLinkId: state.photoLinkId } })),
  );
}),

// GOOD — named state outside, concatMap for sequential ordering, of(face) + closure
const recognizedFacesPipeState = { photoLinkId: null as string | null, cropLinkId: null as string | null };

concatMap(face => of(face).pipe(
  switchMap(() => from(new Aes256Cbc().encrypt(face.eventImageUUID))),
  switchMap(encryptedLink => context.insertRecords({ fields: { link: encryptedLink } })),
  tap(({ id }) => Object.assign(recognizedFacesPipeState, { photoLinkId: id })),
  switchMap(() => context.updateRecords({ fields: { photoLinkId: recognizedFacesPipeState.photoLinkId } })),
)),
```

---

## Examples

### Refactoring: nested pipes → flat stream

**Before:**

```typescript
getData$.pipe(
  switchMap((data) => {
    return this.service.getAdditionalData(data.id).pipe(
      switchMap((additionalData) => {
        return this.anotherService.processData(additionalData).pipe(
          map((processedData) => {
            return { originalData: data, processedData };
          })
        );
      })
    );
  })
);
```

**After:**

```typescript
const state = {
  data: null,
  additionalData: null,
  processedData: null,
};

of(true).pipe(
  switchMap(() => getData$),
  tap(data => Object.assign(state, { data })),
  switchMap(() => this.service.getAdditionalData$(state.data.id)),
  tap(additionalData => Object.assign(state, { additionalData })),
  switchMap(() => this.anotherService.processData$(state.data, state.additionalData)),
  tap(processedData => Object.assign(state, { processedData })),
  map(() => ({ originalData: state.data, processedData: state.processedData })),
  catchError((error) => {
    console.error('Error:', error);
    return of({ error });
  }),
)
```

### Ternary operator instead of `iif`

**Before:**

```typescript
of(true).pipe(
  switchMap(() => getData$),
  switchMap(data =>
    iif(
      () => data.condition,
      this.service.doSomething(data),
      this.service.doSomethingElse(data)
    )
  )
);
```

**After:**

```typescript
of(true).pipe(
  switchMap(() => getData$),
  switchMap(data => data.condition ? this.service.doSomething$(data) : this.service.doSomethingElse$(data)),
);
```

### Guard pattern with `EMPTY` + `defaultIfEmpty`

Use `EMPTY` for early exits from the pipe (guard conditions) and `defaultIfEmpty` at the end to return a default value. This avoids union types in subsequent operators.

**Key difference:**
- `EMPTY` — completes the stream without a value → all subsequent operators are **skipped**
- `of(void 0)` — continues the stream without meaningful data → all subsequent operators **execute**

```typescript
// BAD — returning fallback from switchMap creates a union type,
// requiring type checks at every subsequent step
switchMap(items => {
  if (!items.length) return of(fallbackQuery); // type: IWantedPerson | Query
  return of(items[0]);
}),
switchMap(itemOrQuery => { ... }),

// GOOD — EMPTY skips the entire chain, ternary operator used
switchMap(items => !items.length ? EMPTY : of(items[0])),
tap(item => Object.assign(state, { item })),
switchMap(() => processItem$(state.item)),          // skipped if EMPTY above; item is always IWantedPerson
switchMap(() => sendToExternalSystem$(state.item)), // skipped if EMPTY above
defaultIfEmpty(fallbackQuery),                      // returns fallback if EMPTY above
```

### `EMPTY` and `defaultIfEmpty`

```typescript
of(true).pipe(
  switchMap(() => getData$),
  switchMap(data => data.condition ? this.service.doSomething$(data) : EMPTY),
  // Skip these switchMaps if condition was not met
  switchMap(() => this.service.doSomethingIfNotEmpty1$),
  switchMap(() => this.service.doSomethingIfNotEmpty2$),
  switchMap(() => this.service.doSomethingIfNotEmpty3$),
  // Default value if EMPTY was returned above
  defaultIfEmpty({ defaultValue: null }),
  // Continue execution
  switchMap(() => this.service.doSomethingElse$),
);
```
