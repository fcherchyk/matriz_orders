# python
import os
import json
import re
from pathlib import Path

import requests
from dotenv import load_dotenv

from service.matriz_requester import MatrizRequester

load_dotenv()


class MatrizSession:

    CONFIG_PATH = Path('data/matriz_config.json')

    def __init__(self):
        self.session_id = None
        self.conn_id = None
        self.account_id = None
        self.min_caucion_days = None  # Se obtiene del broker o se configura manualmente
        self.HOST = os.getenv('MATRIZ_HOST', 'matriz.eco.xoms.com.ar')
        self.WS_HOST = os.getenv('MATRIZ_HOST', 'matriz.eco.xoms.com.ar')
        self.PROTOCOL = os.getenv('MATRIZ_PROTOCOL', 'https')
        self.BASE_URL = f"{self.PROTOCOL}://{self.HOST}"
        self.USR = os.getenv('MATRIZ_USERNAME')
        self.PASS = os.getenv('MATRIZ_PASSWORD')
        if not self.USR or not self.PASS:
            raise ValueError("MATRIZ_USERNAME y MATRIZ_PASSWORD deben estar configurados en .env")
        self.requester = MatrizRequester()
        self.load_local_config()
        self.login()

    def login(self):
        response = requests.get(self.BASE_URL, timeout=10)
        self.requester.cookies = response.cookies
        # obtain profile (may populate session_id, conn_id, account_id)
        self.profile()
        # perform auth
        self.requester.http_post('auth/login',
                                f'{{"username": "{self.USR}","password": "{self.PASS}"}}')
        # refresh profile after login
        self.profile()
        # fetch ref-data to determine min caucion days
        self.fetch_ref_data()

    def profile(self):
        response = self.requester.http_get('api/v2/profile')
        if response is None:
            print(f"[MatrizSession] profile() - response is None")
            return
        if response.get('sessionId'):
            self.session_id = response['sessionId']
            print(f"[MatrizSession] profile() - session_id: {self.session_id[:20]}...")
        if response.get('connectionId'):
            self.conn_id = response['connectionId']
            print(f"[MatrizSession] profile() - conn_id: {self.conn_id}")
        # try common keys for account id
        if response.get('accountId'):
            self.account_id = response.get('accountId')
            print(f"[MatrizSession] profile() - account_id: {self.account_id}")
        elif isinstance(response.get('account'), dict) and response['account'].get('id'):
            self.account_id = response['account']['id']
            print(f"[MatrizSession] profile() - account_id: {self.account_id}")
        elif response.get('id'):
            self.account_id = response.get('id')
            print(f"[MatrizSession] profile() - account_id: {self.account_id}")

    def fetch_ref_data(self):
        """
        Gets /api/v2/ref-data and determines minimum caucion days from keys like MERV_PESOS_1D.
        Only updates min_caucion_days if it's currently None (not configured manually or loaded from config).
        If fetch fails or finds no days, sets default value of 1.
        """
        # Solo actualizar si el valor es None (no ha sido configurado)
        if self.min_caucion_days is not None:
            print(f"[MatrizSession] fetch_ref_data() - min_caucion_days ya configurado en {self.min_caucion_days}, omitiendo")
            return

        try:
            response = self.requester.http_get('api/v2/ref-data')
        except Exception as e:
            print(f"[MatrizSession] fetch_ref_data() - error calling ref-data: {e}")
            # Si falla y no hay valor, poner 1 por defecto
            print(f"[MatrizSession] fetch_ref_data() - estableciendo valor por defecto: 1 día")
            self.set_min_caucion_days(1, persist=True)
            return

        if not response:
            print(f"[MatrizSession] fetch_ref_data() - empty response, estableciendo valor por defecto: 1 día")
            self.set_min_caucion_days(1, persist=True)
            return

        # Normalize to a list of items that may contain 'id' fields
        items = []
        if isinstance(response, list):
            items = response
        elif isinstance(response, dict):
            # common wrappers
            if 'data' in response and isinstance(response['data'], list):
                items = response['data']
            elif 'items' in response and isinstance(response['items'], list):
                items = response['items']
            else:
                # response might be a dict of id->value
                items = []
                for k, v in response.items():
                    if isinstance(v, dict):
                        items.append(v)
                    else:
                        items.append({'id': k, 'value': v})

        days_found = []
        for it in items:
            if not isinstance(it, dict):
                continue
            key = it.get('id') or it.get('key') or it.get('code')
            if not key or not isinstance(key, str):
                continue
            if key.startswith('MERV_PESOS_'):
                suffix = key.split('MERV_PESOS_', 1)[1]
                m = re.match(r'(\d+)D', suffix, flags=re.IGNORECASE)
                if m:
                    try:
                        days_found.append(int(m.group(1)))
                    except ValueError:
                        continue

        if days_found:
            new_min = min(days_found)
            print(f"[MatrizSession] fetch_ref_data() - found caucion days options {sorted(set(days_found))}, min -> {new_min}")
            self.set_min_caucion_days(new_min, persist=True)
        else:
            print(f"[MatrizSession] fetch_ref_data() - no MERV_PESOS_* keys found, estableciendo valor por defecto: 1 día")
            self.set_min_caucion_days(1, persist=True)

    def refresh(self):
        """Refresca la sesión haciendo un nuevo login"""
        print(f"[MatrizSession] refresh() - Refrescando sesión...")
        print(f"[MatrizSession] refresh() - session_id anterior: {self.session_id[:20] if self.session_id else 'None'}...")
        self.login()
        print(f"[MatrizSession] refresh() - session_id nuevo: {self.session_id[:20] if self.session_id else 'None'}...")

    # Local config persistence so the setting can be edited in UI and reused
    def load_local_config(self):
        try:
            if self.CONFIG_PATH.exists():
                with self.CONFIG_PATH.open('r', encoding='utf-8') as fh:
                    cfg = json.load(fh)
                    md = cfg.get('min_caucion_days')
                    if isinstance(md, int) and md > 0:
                        self.min_caucion_days = md
                    # optionally load stored account_id
                    if cfg.get('account_id'):
                        self.account_id = cfg.get('account_id')
        except Exception as e:
            print(f"[MatrizSession] load_local_config() - error: {e}")

    def save_local_config(self):
        try:
            self.CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            cfg = {
                'min_caucion_days': self.min_caucion_days,
                'account_id': self.account_id
            }
            with self.CONFIG_PATH.open('w', encoding='utf-8') as fh:
                json.dump(cfg, fh, ensure_ascii=False, indent=2)
            print(f"[MatrizSession] save_local_config() - saved {self.CONFIG_PATH}")
        except Exception as e:
            print(f"[MatrizSession] save_local_config() - error: {e}")

    def set_min_caucion_days(self, days: int, persist: bool = False):
        """Set the minimum days (can be called from UI)."""
        try:
            days_int = int(days)
            if days_int < 1:
                raise ValueError("days must be >= 1")
            self.min_caucion_days = days_int
            if persist:
                self.save_local_config()
            print(f"[MatrizSession] set_min_caucion_days() - set to {self.min_caucion_days}")
        except Exception as e:
            print(f"[MatrizSession] set_min_caucion_days() - invalid value {days}: {e}")

    def get_account_info(self):
        """Return info to display in the settings UI: account id and min caucion days."""
        return {
            'account_id': self.account_id,
            'min_caucion_days': self.min_caucion_days
        }