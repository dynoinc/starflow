"""termichat assistant"""

load("json", json_decode="decode")

tools = [
        {
            "type": "function",
            "function": {
                "name": "memory_store",
                "description": "Store a memory or important information for later recall",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "memory": {
                            "type": "string",
                            "description": "The memory or information to store"
                        }
                    },
                    "required": ["memory"]
                }
            }
        }
    ]

def create_completion_params(messages):
    """Create OpenAI ChatCompletionNewParams"""
    return {
        "messages": messages,
        "model": "gpt-4o-mini",
        "tools": tools
    }

def extract_message_from_completion(result):
    """Extract message from OpenAI ChatCompletion response"""
    if not result.get("choices") or len(result["choices"]) == 0:
        return None
    return result["choices"][0]["message"]

def create_assistant_message(message):
    """Create assistant message for conversation history"""
    assistant_message = {"role": "assistant"}
    if message.get("content"):
        assistant_message["content"] = message["content"]
    if message.get("tool_calls"):
        assistant_message["tool_calls"] = message["tool_calls"]
    return assistant_message

def execute_tool_calls(ctx, tool_calls):
    """Execute tool calls and return tool messages"""
    tool_messages = []
    for tool_call in tool_calls:
        if tool_call["function"]["name"] == "memory_store":
            # Parse arguments and call memory.store
            args = json_decode(tool_call["function"]["arguments"])
            memory_result = memory.store(ctx, {"memory": args["memory"]})
            
            # Create tool response message
            tool_message = {
                "role": "tool",
                "tool_call_id": tool_call["id"],
                "content": "Memory stored successfully" if memory_result["success"] else "Failed to store memory"
            }
            tool_messages.append(tool_message)
    return tool_messages

def main(ctx, input):
    # Start conversation with user message
    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": input["message"]}
    ]
    
    # Tool call loop (up to 5 iterations)
    for range(5):
        # Create completion request and call OpenAI
        completion_params = create_completion_params(messages)
        result = openai.complete(ctx, completion_params)
        
        # Extract message from response
        message = extract_message_from_completion(result)
        if not message:
            return {"response": "No response from the model"}
        
        # Add assistant message to conversation
        assistant_message = create_assistant_message(message)
        messages.append(assistant_message)
        
        # Handle tool calls if present
        if message.get("tool_calls"):
            tool_messages = execute_tool_calls(ctx, message["tool_calls"])
            messages.extend(tool_messages)
            continue
        else:
            # No tool calls, return the response
            return {"response": message.get("content", "")}
    
    # If we've exhausted retries, return the last response
    return {"response": message.get("content", "")} 