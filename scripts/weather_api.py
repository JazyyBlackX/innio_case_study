
# %% 
import requests

response = requests.get("http://api.openweathermap.org/geo/1.0/direct?q=London&limit=5&appid=bc2a38adcc749f24fe08205c68bb5c33")
print(response.status_code)
print(response.json())