import requests

url = "https://rsyxpli63a.execute-api.us-east-1.amazonaws.com/default/openai-proxy"

payload = {
    "prompt": "Explain Fitts' Law in 2 sentences."
}

headers = {
    "Content-Type": "application/json"
}

resp = requests.post(url, json=payload, headers=headers)
print(resp.json())
