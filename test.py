import requests

url = ""

payload = {
    "prompt": "Explain Fitts' Law in 2 sentences."
}

headers = {
    "Content-Type": "application/json"
}

resp = requests.post(url, json=payload, headers=headers)
print(resp.json())
