//
//  WriteBatch.mm
//
//  Copyright 2013 Storm Labs.
//  See LICENCE for details.
//

#import <leveldb/db.h>
#import <leveldb/write_batch.h>
#import <os/lock.h>

#import "LDBWriteBatch.h"
#import "LDBCommon.h"
#import "LevelDB.h"

@interface LDBWritebatch () {
    leveldb::WriteBatch _writeBatch;
    id _db;
}

@property (readonly) leveldb::WriteBatch writeBatch;

@end

@implementation LDBWritebatch {
    os_unfair_lock _locker;
}

@synthesize writeBatch = _writeBatch;
@synthesize db = _db;

+ (instancetype) writeBatchFromDB:(id)db {
    id wb = [[self alloc] init];
    ((LDBWritebatch *)wb)->_db = db;
    return wb;
}

- (instancetype) init {
    self = [super init];
    if (self) {
        _locker = OS_UNFAIR_LOCK_INIT;
    }
    return self;
}
- (void)dealloc {
    if (_db) {
        _db = nil;
    }
}

- (void) removeObjectForKey:(id)key {
    AssertKeyType(key);
    leveldb::Slice k = KeyFromStringOrData(key);
    os_unfair_lock_lock(&_locker);
    _writeBatch.Delete(k);
    os_unfair_lock_unlock(&_locker);
}
- (void) removeObjectsForKeys:(NSArray *)keyArray {
    [keyArray enumerateObjectsUsingBlock:^(id obj, NSUInteger idx, BOOL *stop) {
        [self removeObjectForKey:obj];
    }];
}
- (void) removeAllObjects {
    [_db enumerateKeysUsingBlock:^(LevelDBKey *key, BOOL *stop) {
        [self removeObjectForKey:NSDataFromLevelDBKey(key)];
    }];
}

- (void) setData:(NSData *)data forKey:(id)key {
    AssertKeyType(key);
    os_unfair_lock_lock(&_locker);
    leveldb::Slice lkey = KeyFromStringOrData(key);
    _writeBatch.Put(lkey, SliceFromData(data));
    os_unfair_lock_unlock(&_locker);
}
- (void) setObject:(id)value forKey:(id)key {
    AssertKeyType(key);
    os_unfair_lock_lock(&_locker);
    leveldb::Slice k = KeyFromStringOrData(key);
    LevelDBKey lkey = GenericKeyFromSlice(k);
    
    NSData *data = ((LevelDB *)_db).encoder(&lkey, value);
    leveldb::Slice v = SliceFromData(data);
    
    _writeBatch.Put(k, v);
    os_unfair_lock_unlock(&_locker);
}
- (void) setValue:(id)value forKey:(NSString *)key {
    [self setObject:value forKey:key];
}
- (void) setObject:(id)value forKeyedSubscript:(id)key {
    [self setObject:value forKey:key];
}
- (void) addEntriesFromDictionary:(NSDictionary *)dictionary {
    [dictionary enumerateKeysAndObjectsUsingBlock:^(id key, id obj, BOOL *stop) {
        [self setObject:obj forKey:key];
    }];
}

- (BOOL) apply {
    return [_db applyWritebatch:self];
}

@end
