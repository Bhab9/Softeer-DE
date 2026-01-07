from typing import List, Dict, Union
from ollama import chat, pull, ChatResponse
import logging
import subprocess
from typing import Any
from langchain_ollama import ChatOllama

class OllamaLM:
    def __init__(self, model_name, port, max_token, seed):
        self.model_name = model_name
        self.port = port
        self.chat_model = ChatOllama(model=model_name, 
                                base_url=f"http://localhost:{port}",
                                num_predict=max_token,
                                seed=seed)
        self.call_cnt = 0
        self.token_cnt = 0
        self.response_cache = {}

    def query(self, query: str) -> Dict:        
        message = self.chat_model.invoke(query)
        self.call_cnt += 1
        if message.usage_metadata is not None:
            self.token_cnt += message.usage_metadata["output_tokens"]
        else:
            print(message)
        logging.info(f"cumul_call_cnt: {self.call_cnt}, cumul_token_cnt: {self.token_cnt}")

        response = {}
        response["generated_text"] = message.content
        responses = []
        responses.append(response)

        return responses
    
        # INFO: 1
        # if query in self.response_cache:
        #     return self.response_cache[query]

        # responses = []
        # query = f"<s><<SYS>>You are a helpful assistant. Always follow the instructions precisely and output the response exactly in the requested format.<</SYS>>\n\n[INST] {query} [/INST]"

        # # Use ollama.chat() to get the response
        # response: ChatResponse = chat(model=self.model_name, messages=[
        #     {'role': 'user', 'content': query}
        # ])
        # responses.append({
        #     "generated_text": response.message.content.strip()
        # })
        
        # # if self.cache:
        # self.response_cache[query] = responses
        
        # return responses


    def get_response_texts(self, query_responses: List[Dict]) -> List[str]:
        return [query_response["generated_text"] for query_response in query_responses]