import sys
import os 
import time
import itertools
from pyspark.context import SparkContext
from collections import Counter, defaultdict

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Initialize SparkContext
sc = SparkContext("local[*]", "task1")


# build the utils
def generate_canditates(prev_frequent, k):
    '''
    prev_frequent: list of frequent (k-1) itemsets
    k: the size of the candidates
    '''
    prev_frequent = sorted(prev_frequent)
    candidates = set()
    n = len(prev_frequent)

    for i in range(n):
        for j in range(i+1, n):
            l1 = prev_frequent[i]
            l2 = prev_frequent[j]

            if prev_frequent[i][:k-2] == prev_frequent[j][:k-2]:
                new_candidate = tuple(sorted(set(l1) | set(l2)))
                if all(tuple(sorted(c)) in prev_frequent for c in itertools.combinations(new_candidate, k-1)):
                    candidates.add(new_candidate)

    return list(candidates)


def get_frequent_candidates(candidates, baskets, support):
    count = {}

    for candidate in candidates:
        cand_set = set(candidate)
        for basket in baskets:
            if cand_set.issubset(basket):
                count[candidate] = count.get(candidate, 0) + 1

    frequent_items = [candidate for candidate, count in count.items() if count >= support]
    return frequent_items

def apriori(baskets_input):
    baskets_list = list(baskets_input)
    local_support = support * (len(baskets_list)/NUM_total_baskets)

    # finding frequent 1-itemsets
    items = [item for basket in baskets_list for item in basket]
    items_count = Counter(items)
    frequent_1_items = [item for item, count in items_count.items() if count >= local_support]

    # finding frequent 2-itemsets
    candidates = list(itertools.combinations(sorted(frequent_1_items), 2))
    frequent_2_items = get_frequent_candidates(candidates, baskets_list, local_support)

    # finding frequent k-itemsets
    k = 3
    prev_frequent = frequent_2_items
    frequent_itemsets = frequent_1_items + frequent_2_items

    while True:
        candidates = generate_canditates(prev_frequent, k)
        frequent_k_items = get_frequent_candidates(candidates, baskets_list, local_support)
        prev_frequent = frequent_k_items
        if not frequent_k_items:
            break
        frequent_itemsets.extend(frequent_k_items)
        k += 1

    output = []
    for itemset in frequent_itemsets:
        if isinstance(itemset, str):
            output.append(( (itemset,), 1 ))
        else:
            output.append(( itemset, 1 ))
            
    return output

def verify_frequent_items(frequent_itemsets, baskets, support):
    count = defaultdict(int)

    for basket in baskets:
        if all(item in basket for item in frequent_itemsets):
            count[frequent_itemsets] += 1
    
    frequent_itemsets = [(tuple(sorted(itemset)), count) for itemset, count in count.items() if count >= support]
    return frequent_itemsets

def format_string(input_tuple, size, output):
    if len(input_tuple) > size:
        return output + "\n\n"
    if len(input_tuple) == 1:
        output += f"('{input_tuple[0]}'),"
    else: 
        output += f"{input_tuple},"
    return output


if __name__ == "__main__":
    start_time = time.time()

    # Set up paths and parameters
    case = int(sys.argv[1])
    support = int(sys.argv[2])
    input_file_path = sys.argv[3]
    Output_file_path = sys.argv[4]

    # Read the input file and create baskets
    lines = sc.textFile(input_file_path)
    header = lines.first()
    rdd = lines.filter(lambda line: line != header).map(lambda line: line.split(","))

    # Preprocessing data  based on the case 
    if case == 1:
        baskets = rdd.map(lambda x: (x[0], x[1])) \
                        .groupByKey() \
                        .mapValues(list)\
                        .map(lambda x: x[1])
    elif case == 2:
        baskets = rdd.map(lambda x: (x[1], x[0])) \
                        .groupByKey() \
                        .mapValues(list) \
                        .map(lambda x: x[1])

    # SON Algorithm
    NUM_total_baskets = baskets.count()
    basket_list = baskets.collect()
    
    pass1_mapper = baskets.mapPartitions(apriori)
    pass1_reducer = pass1_mapper.reduceByKey(lambda x, y: x + y)

    pass2_mapper = pass1_reducer.map(lambda x: verify_frequent_items(x[0], basket_list, support)).flatMap(lambda x: x)
    pass2_reducer = pass2_mapper.filter(lambda x: x[1] >= support).map(lambda x: x[0]).collect()

    pass1_final = ""
    pass1_result = sorted(sorted(pass1_reducer.map(lambda x: x[0]).collect()), key=lambda element: (len(element), element))
    size = len(pass1_result[0])
    for x in pass1_result:
        if len(x) != size:
            pass1_final = pass1_final[:-1] + "\n\n"
            size = len(x)
        pass1_final = format_string(x, size, pass1_final)

    print(f"Time taken: {time.time() - start_time} seconds")

    pass2_final = ""
    pass2_result = sorted(sorted(pass2_reducer),
                           key=lambda element: (len(element), element))
    size = len(pass2_result[0])
    for x in pass2_result:
        if len(x) != size:
            pass2_final = pass2_final[:-1] + "\n\n"
            size = len(x)
        pass2_final = format_string(x, size, pass2_final)
    
    with open(Output_file_path, "w") as f:
        f.write("Candidates:\n" + pass1_final[:-1] + "\n\nFrequent Itemsets:\n" + pass2_final[:-1])

    print("Duration:", time.time() - start_time, "seconds")
    


    