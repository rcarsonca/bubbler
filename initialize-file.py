import json

bubbler_main = "0"
statemachine = "0"
auto_bubble = "0"
bubbler_1 = "0"
bubbler_2 = "0"
bubbler_3 = "0"
danger_lights = "0"

jsonData = {"mainkey": bubbler_main, "statekey": statemachine, "autokey": auto_bubble, "b1key": bubbler_1, "b2key": bubbler_2, "b3key": bubbler_3, "dangerkey": danger_lights}

try:
    with open('savedata.json', 'w') as f:
        json.dump(jsonData, f)
except:
        print ("Error! Could not save")
