## Known Limitations

**Sliding window race condition**: The count check and timestamp add are not atomic. 
Two simultaneous requests could both pass the limit check before either is recorded. 
A Lua script executed atomically in Redis would fix this in production.
