import dagster as dg
import requests
from google.genai.client import AsyncClient, Client
from google.genai.types import GenerateContentResponse
from requests import Response
from requests.auth import HTTPBasicAuth


class JiraClient(dg.ConfigurableResource):
    base_url: str = dg.EnvVar("BASE_URL")
    username: str = dg.EnvVar("ATLASSIAN_EMAIL")
    api_token: str = dg.EnvVar("ATLASSIAN_API_KEY")

    @property
    def auth(self) -> HTTPBasicAuth:
        return HTTPBasicAuth(self.username, self.api_token)

    @property
    def headers(self) -> dict[str, str]:
        return {"Accept": "application/json"}

    def _build_url(self, endpoint: str) -> str:
        if endpoint.startswith(("http://", "https://")):
            return endpoint
        return f"{self.base_url.rstrip('/')}/{endpoint.lstrip('/')}"

    def get(self, endpoint: str, params: dict | None = None) -> Response:
        url = self._build_url(endpoint)
        response = requests.get(
            url, auth=self.auth, params=params, headers=self.headers
        )
        response.raise_for_status()
        return response

    def post(self, endpoint: str, data: dict | None = None) -> Response:
        url = self._build_url(endpoint)
        response = requests.post(url, auth=self.auth, json=data, headers=self.headers)
        response.raise_for_status()
        return response

    def search_issues(
        self,
        jql: str,
        fields: list[str] | None = None,
        max_results: int = 50,
        next_page_token: str | None = None,
    ) -> dict:
        """Searches Jira issues using JQL via REST API v3 search/jql endpoint."""
        payload: dict = {
            "jql": jql,
            "maxResults": max_results,
        }
        if fields:
            payload["fields"] = fields
        if next_page_token:
            payload["nextPageToken"] = next_page_token

        try:
            response = self.post("rest/api/3/search/jql", data=payload)
            return response.json()
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code in (404, 405):
                response = self.post("rest/api/3/search", data=payload)
                return response.json()
            raise


class LLMClient(dg.ConfigurableResource):
    api_key: str = dg.EnvVar("GOOGLE_API_KEY")
    model: str = "gemini-3.5-flash"

    def get_client(self) -> AsyncClient:
        return Client(api_key=self.api_key).aio

    async def generate(self, content: str, config: dict | None = None) -> GenerateContentResponse:
        client = self.get_client()
        response = await client.models.generate_content(
            model=self.model, contents=content, config=config
        )
        return response


@dg.definitions
def resources() -> dg.Definitions:
    return dg.Definitions(
        resources={"jira_client": JiraClient(), "llm_client": LLMClient()}
    )
