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

def create_completion_params(messages, include_tools=True):
    """Create OpenAI ChatCompletionNewParams"""
    params = {
        "messages": messages,
        "model": "gpt-4o-mini"
    }
    if include_tools:
        params["tools"] = tools
    return params

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
            args = json_decode(tool_call["function"]["arguments"])
            memory_result = memory.store(ctx, {"memory": args["memory"]})
            
            success_msg = "Memory stored successfully"
            error_msg = "Failed to store memory"
            content = success_msg if memory_result["success"] else error_msg
            
            tool_message = {
                "role": "tool",
                "tool_call_id": tool_call["id"],
                "content": content
            }
            tool_messages.append(tool_message)
    return tool_messages

def load_recent_memories(ctx):
    """Load the 5 most recent memories"""
    memory_result = memory.restore(ctx, {"count": 5})
    return memory_result.get("memories", [])

def main(ctx, input):
    system_prompt = "You are a helpful assistant.\n\n"

    # Add memories to the system message
    memories = load_recent_memories(ctx)
    if memories:
        system_prompt += '''
        Recent memories from our past conversations:
        {memories_text}
        Use these memories to provide more contextual and personalized assistance.
        '''.format(memories_text="\n".join(["- " + mem for mem in memories]))

    messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": input["message"]}
    ]

    max_iterations = 5
    for iteration in range(max_iterations):
        is_final_iteration = (iteration == max_iterations - 1)

        if is_final_iteration:
            messages.append({
                "role": "system", 
                "content": '''
                This is the final iteration of the conversation. 
                Please provide a comprehensive answer and include suggestions on: 
                1. What progress has been made so far 
                2. What specific next steps the user should take 
                3. Any important considerations or potential challenges 
                4. How the user can continue or resume this work
                '''
            })

        include_tools = not is_final_iteration
        completion_params = create_completion_params(messages, include_tools)
        result = openai.complete(ctx, completion_params)
        
        message = extract_message_from_completion(result)
        if not message:
            return {"response": "No response from the model"}
        
        assistant_message = create_assistant_message(message)
        messages.append(assistant_message)
        
        has_tool_calls = message.get("tool_calls")
        if not has_tool_calls:
            break

        tool_messages = execute_tool_calls(ctx, message["tool_calls"])
        messages.extend(tool_messages)
    
    return {"response": messages[-1].get("content", "")} 