import re
import json
from typing import List, Dict, Any
from rapidfuzz import fuzz
from elasticsearch import Elasticsearch, RequestsHttpConnection, exceptions
import click

class ElasticSearchQuery:
    def __init__(self, host: str = "localhost", port: int = 9200, index_name: str = "drug_data"):
        self.index_name = index_name
        try:
            self.es = Elasticsearch(
                hosts=[{"host": host, "port": port}],
                scheme="http",
                port=port,
                verify_certs=False,
                connection_class=RequestsHttpConnection
            )
            if not self.es.ping():
                raise Exception("Could not connect to Elasticsearch")
        except Exception as e:
            raise Exception(f"Error initializing Elasticsearch client: {e}")

    def get_candidate_docs(self, size: int = 50) -> List[Dict[str, Any]]:
        query_body = {
            "size": size,
            "query": {"match_all": {}}
        }
        try:
            resp = self.es.search(index=self.index_name, body=query_body)
            return resp.get("hits", {}).get("hits", [])
        except exceptions.ElasticsearchException as e:
            raise Exception(f"Error retrieving candidate documents: {e}")

    def aggregate_document_text(self, source: dict) -> str:
        text = source.get("name", "")
        if "sections" in source and isinstance(source["sections"], list):
            for sec in source["sections"]:
                if sec and isinstance(sec, dict):
                    text += " " + sec.get("title", "") + " " + sec.get("content", "")
        return text

    def compute_global_similarity(self, query: str, text: str) -> float:
        return fuzz.token_set_ratio(query, text)

    def compute_detail_score(self, query: str, text: str) -> float:
        query_tokens = re.findall(r'\w+', query.lower())
        doc_tokens = re.findall(r'\w+', text.lower())
        token_best_scores = []
        for token in query_tokens:
            best = 0
            for dt in doc_tokens:
                score = fuzz.ratio(token, dt)
                if score > best:
                    best = score
            token_best_scores.append(best)
        if token_best_scores:
            return sum(token_best_scores) / len(token_best_scores)
        return 0.0

    def get_matching_details(self, query: str, text: str, threshold: int = 70) -> Dict[str, Dict[str, Any]]:
        query_tokens = re.findall(r'\w+', query.lower())
        doc_tokens = re.findall(r'\w+', text.lower())
        details = {}
        for q in query_tokens:
            best_match = None
            best_score = 0
            for dt in doc_tokens:
                score = fuzz.ratio(q, dt)
                if score >= threshold and score > best_score:
                    best_score = score
                    best_match = dt
            details[q] = {"best_match": best_match, "score": best_score}
        return details

    def fuzzy_search(self, user_query: str, top_n: int = 10) -> List[Dict[str, Any]]:
        candidates = self.get_candidate_docs(size=50)
        results = []
        for hit in candidates:
            doc_id = hit.get("_id")
            source = hit.get("_source", {})
            full_text = self.aggregate_document_text(source)
            global_similarity = self.compute_global_similarity(user_query, full_text)
            detail_score = self.compute_detail_score(user_query, full_text)
            final_score = 0.7 * global_similarity + 0.3 * detail_score
            matching_details = self.get_matching_details(user_query, full_text)
            matched_words = [detail["best_match"] for detail in matching_details.values() if detail["best_match"] is not None]
            results.append({
                "doc_id": doc_id,
                "global_similarity": global_similarity,
                "detail_similarity": detail_score,
                "final_score": final_score,
                "matching_details": matching_details,
                "document": source
            })
        results.sort(key=lambda x: x["final_score"], reverse=True)
        return results[:top_n]

@click.command()
@click.option("--query", help="Search query string. For example: --query 'Pentamidine injection'", required=True)
@click.option("--top_n", default=10, help="Number of top results to return", type=int)
@click.option("--output-file", default="results.json", help="Path to output JSON file")
def main(query, top_n, output_file="/elastic_search/results/results.json"):
    if not query.strip():
        click.echo("Error: --query option must be provided. Example usage: --query 'Pentamidine injection'")
        raise click.Abort()
    try:
        es_query = ElasticSearchQuery(host="localhost", port=9200, index_name="drug_data")
        results = es_query.fuzzy_search(user_query=query, top_n=top_n)
        click.echo("\n--- Top Search Results ---")
        for idx, res in enumerate(results, start=1):
            click.echo("-" * 50)
            click.echo(f"{idx}. DOC_ID: {res['doc_id']}")
            click.echo(f"   Global Similarity: {res['global_similarity']:.2f}%")
            click.echo(f"   Detail Similarity: {res['detail_similarity']:.2f}%")
            click.echo(f"   Final Score: {res['final_score']:.2f}")
            click.echo(f"   Name: {res['document'].get('name')}")
            click.echo(f"   URL: {res['document'].get('url')}")
            click.echo("   Matching Details:")
            for token, detail in res["matching_details"].items():
                if detail["best_match"]:
                    click.echo(f"      - Query token '{token}' matched '{detail['best_match']}' with score {detail['score']}%")
                else:
                    click.echo(f"      - Query token '{token}' did not have a match above threshold.")
            click.echo("-" * 50)
        with open("./elastic_search/results/results.json", "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        click.echo(f"\nJSON results written to: {output_file}")
    except Exception as e:
        click.echo(f"Error: {e}")

if __name__ == "__main__":
    main()