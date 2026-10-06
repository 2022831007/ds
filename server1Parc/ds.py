import multiprocessing as mp
import os
num_reducers=3
DOCUMENTS = [
    "distributed systems are fun",
    "map reduce is a programming model",
    "the master splits input into map tasks",
    "reduce tasks sum the counts from map tasks",
    "fun fun fun distributed computing",
]

def map_task(doc_id,text):
    print(f"[Map worker pid ={os.getpid()} mapping doc {doc_id}")
    pairs = []
    for word in text.split():
        pairs.append((word,1))
    return pairs    

def shuffle(all_mapped_pairs,num_reducers):
    groups = {}
    for pairs in all_mapped_pairs:
        for word,count in pairs:
            groups.setdefault(word,[]).append(count)
    buckets = [{} for _ in range(num_reducers)]
    for word,counts in groups.items():
        bucket_id = hash(word)%num_reducers
        buckets[bucket_id][word]=counts
    return buckets   

def reduce_task(bucket_id,bucket):
    print(f"[Reduce worker pid ={os.getpid()} Reducing doc {bucket_id}")
    result = {}
    for word,count in bucket.items():
        result[word] = sum(count)
    return result 

def main():
    print(f"[Master] splitting input into",len(DOCUMENTS),"map task")
    with mp.Pool(processes=4) as pool:
        map_results = pool.starmap(
            map_task,[(i,doc) for i ,doc in enumerate(DOCUMENTS)]
        )
    buckets = shuffle(map_results,num_reducers)
    with mp.Pool(processes=num_reducers) as pool:
         reduce_results = pool.starmap(
             reduce_task,list(enumerate(buckets))
         )
    final_counts = {}
    for partial in reduce_results:
         final_counts.update(partial)


    for word,count in sorted(final_counts.items(), key = lambda kv:-kv[1]):
         print(f"{word:12s}{count}")
         
if __name__=="__main__":
    main()


