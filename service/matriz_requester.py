import datetime
import random
import requests

class MatrizRequester:

    def __init__(self):
        self.cookies = {}
        self.csrf_token = None
        self.HOST = 'matriz.eco.xoms.com.ar'
        self.PROTOCOL = 'https'
        self.BASE_URL = f"{self.PROTOCOL}://{self.HOST}"
        self.ACCOUNT = '73739'
        self.WS_HOST = 'matriz.eco.xoms.com.ar'

    @property
    def account(self):
        """Alias para ACCOUNT para acceso consistente"""
        return self.ACCOUNT

    def prepare_headers(self):
        return {"Content-Type": "application/json",
                   "Accept": "application/json, text/plain, */*",
                   'Origin': f'{self.BASE_URL}',
                   'Referer': f'{self.BASE_URL}/principal/favoritos',
                   'Accept-Language': 'en,es;q=0.9',
                   'sec-ch-ua': '"Chromium";v="126", "Not/A)Brand";v="8", "Google Chrome";v="126"',
                   "sec-ch-ua-mobile": "?0",
                   'Accept-Encoding': 'gzip, deflate, br, zstd',
                   "Priority": "u=1, i",
                   "Sec-ch-ua-platform": '"macOS"',
                   "Sec-Fetch-Dest": "empty",
                   "Sec-Fetch-Mode": "cors",
                   'Sec-Fetch-Site': 'same-origin',
                   'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, '
                                 'like Gecko) Chrome/126.0.0.0 Safari/537.36',
                   'X-CSRF-TOKEN': self.csrf_token}

    def processResponse(self, response, context=""):
        if response.status_code == 200:
            jsonResponse = response.json()
            self.read_cookies(response, jsonResponse)
            return jsonResponse
        else:
            print(f"[MatrizRequester] {context} ERROR HTTP {response.status_code}", flush=True)
            print(f"[MatrizRequester] {context} Response body: {response.text[:500] if response.text else 'empty'}", flush=True)
            return None

    def http_get(self, resource):
        # Si resource contiene "api/v2" o empieza con "auth/", concatenar directo a BASE_URL
        # De lo contrario, asumir que es un endpoint de cuenta
        if "api/v2" in resource or resource.startswith("auth/"):
            url = f"{self.BASE_URL}/{resource}"
        else:
            url = f"{self.BASE_URL}/api/v2/accounts/{self.ACCOUNT}/{resource}"
        t = random.randint(0, 999998)
        timestamp = int(datetime.datetime.now().timestamp() * 1000)
        n = f"{timestamp}-{t}"
        # Hacer la llamada al servicio
        url = f"{url}&_ds={n}" if "?" in url else f"{url}?_ds={n}"
        response = requests.get(url, headers=self.prepare_headers(), cookies=self.cookies, timeout=10)
        return self.processResponse(response, f"GET {resource}")

    def http_post(self, resource, payload):
        # Si resource contiene "api/v2" o empieza con "auth/", concatenar directo a BASE_URL
        # De lo contrario, asumir que es un endpoint de cuenta
        if "api/v2" in resource or resource.startswith("auth/"):
            url = f"{self.BASE_URL}/{resource}"
        else:
            url = f"{self.BASE_URL}/api/v2/accounts/{self.ACCOUNT}/{resource}"
        print(f"[MatrizRequester] POST {resource}", flush=True)
        print(f"[MatrizRequester] URL: {url}", flush=True)
        print(f"[MatrizRequester] Payload: {payload[:200]}{'...' if len(payload) > 200 else ''}", flush=True)
        print(f"[MatrizRequester] CSRF Token: {self.csrf_token[:20] if self.csrf_token else 'None'}...", flush=True)
        print(f"[MatrizRequester] Cookies: {list(self.cookies.keys())}", flush=True)
        try:
            response = requests.post(url, data=payload, headers=self.prepare_headers(), cookies=self.cookies, timeout=10)
            print(f"[MatrizRequester] Response status: {response.status_code}", flush=True)
            return self.processResponse(response, f"POST {resource}")
        except Exception as e:
            print(f"[MatrizRequester] POST {resource} EXCEPTION: {e}", flush=True)
            import traceback
            traceback.print_exc()
            raise

    def read_cookies(self, response, jsonResponse):
        if jsonResponse.get('csrfToken'):
            self.csrf_token = jsonResponse['csrfToken']
        if response.cookies.get('_mtz_web_key') is not None:
            self.cookies['_mtz_web_key'] = response.cookies['_mtz_web_key']





