import json

import esprima


resp = esprima.parseScript(
            open('index.js', 'r')
                .read()
                .split('features:Q0},')[-1]
                .split(',I9=[];')[0])


def get_key(prop):
    if prop.key.value:
        return prop.key.value.lower()

    if prop.key.name:
        return prop.key.name.lower()

    return 'unknown'


dataset = [
    {get_key(prop): prop.value.value
     for prop in element.properties}
    for element in resp.body[0].expression.right.elements]

with open('data_centres.json', 'w') as f:
    for rec in dataset:
        f.write(json.dumps(rec, sort_keys=True) + '\n')