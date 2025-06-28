"""termichat assistant"""

def main(ctx, input):
    result = openai.complete(ctx, {"prompt": input["message"], "model": "gpt-4o-mini"})
    return {"response": result["completion"]} 