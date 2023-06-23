import requests

def main():
    url = "https://randomuser.me/api/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.text
        
        with open("data.json", "w") as file:
            file.write(data)
            
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)

if __name__ == "__main__":
    main()
