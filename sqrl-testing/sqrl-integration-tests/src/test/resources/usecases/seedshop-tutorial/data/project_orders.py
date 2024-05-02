import json

# Projects out the first item from the nested orders to create a simplified orders file
# for our nutshop example. Leave this file in the folder for future use.

# Read data
with open('orderitems.jsonl', 'r') as f:
    lines = f.readlines()

# Transform data by taking first item and moving it to the top-level object
transformed_data = []
for line in lines:
    data = json.loads(line)
    first_item = data['items'].pop(0)
    del data['items']
    data.update(first_item)
    transformed_data.append(data)

# Write the transformed data to another jsonl file
with open('orders.jsonl', 'w') as f:
    for data in transformed_data:
        f.write(json.dumps(data) + '\n')