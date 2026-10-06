import multiprocessing as mp
import os
import time
map_timeout=2.0
DOCUMENTS = [
    "distributed systems are fun",
    "map reduce is a programming model",
    "the master splits input into map tasks",
    "reduce tasks sum the counts from map tasks",
    "fun fun fun distributed computing",
]
def flaky_map_task(doc_id,text,attempt):
    if doc_id==1 and attempt ==1:
        raise RuntimeError("simulated worker crash")
    
    if doc_id==3:
        time.sleep(3)

    pairs = [(word,1) for word in text.split()] 
    return pairs

def run_map_with_retry(pool,doc_id,text,max_attempt=3):
    for attempt in range(1,max_attempt+1):
        async_result = pool.apply_async(flaky_map_task,(doc_id,text,attempt))
        try:
            return async_result.get(timeout=map_timeout)
        except mp.TimeoutError:
            backup_result = pool.apply_async(flaky_map_task,(doc_id,text,attempt))
            return backup_result.get()
        except Exception as e:
            print("Crashed ({e})")
            continue
    raise RuntimeError(f"failed after {max_attempt} attempts")    
   
def main():
    print("[MASTER] Splitting input into", len(DOCUMENTS), "map tasks")
    with mp.Pool(processes=4) as pool:
        all_pairs = []
        for doc_id,text in enumerate(DOCUMENTS):
          pairs = run_map_with_retry(pool,doc_id,text)
          all_pairs.extend(pairs)

        counts ={}
        for word,c in all_pairs:
            counts[word]  = counts.get(word,0)+c 
        print("\n[MASTER] All map tasks eventually succeeded. FINAL WORD COUNTS:")
    for word, count in sorted(counts.items(), key=lambda kv: -kv[1]):
        print(f"  {word:12s} {count}")    


if __name__ == "__main__":
    main()