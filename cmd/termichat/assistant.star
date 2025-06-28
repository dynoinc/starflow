"""termichat assistant"""

load("json", json_decode="decode")

inbuilt_tools = [
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

def execute_tool_calls(ctx, tool_calls):
    """
    Execute tool calls and return tool messages.

    Args:
        ctx: The context of the assistant run
        tool_calls: The tool calls to execute

    Returns:
        A list of tool messages
    """
    tool_messages = []
    for tool_call in tool_calls:
        function_name = tool_call["function"]["name"]
        args = json_decode(tool_call["function"]["arguments"])
        
        if function_name == "memory_store":
            # Handle memory store as before
            memory_result = memory.store(ctx, {"memory": args["memory"]})
            success_msg = "Memory stored successfully"
            error_msg = "Failed to store memory"
            content = success_msg if memory_result["success"] else error_msg
            
        elif "_" in function_name:
            # Handle MCP tools (format: servername_toolname)
            parts = function_name.split("_", 1)
            server_name = parts[0]
            tool_name = parts[1]
            
            call_result = mcp.call_tool(ctx, {
                "server": server_name,
                "tool": tool_name,
                "arguments": args
            })
            
            if call_result.get("isError"):
                content = "Error: " + str(call_result.get("content", "Unknown error"))
            else:
                content = str(call_result.get("content", ""))
        else:
            content = "Unknown tool: " + function_name
        
        tool_message = {
            "role": "tool",
            "tool_call_id": tool_call["id"],
            "content": content
        }
        tool_messages.append(tool_message)
    
    return tool_messages

def main(ctx, input):
    """
    Main function for the assistant.

    Args:
        ctx: The context of the assistant run
        input: The input message from the user

    Returns:
        A dictionary containing the response from the assistant
    """

    # Load conversation history, memories and MCP tools available to the assistant
    history = conversations.history(ctx, {"count": 5})
    memories = memory.restore(ctx, {"count": 5}).get("memories", [])
    mcp_tools = mcp.list_tools(ctx, {})

    system_prompt = "You are a helpful assistant.\n\n"
    if memories:
        system_prompt += '''
        Recent memories from our past conversations:
        {memories_text}
        Use these memories to provide more contextual and personalized assistance.
        '''.format(memories_text="\n".join(["- " + mem for mem in memories]))

    messages = [{"role": "system", "content": system_prompt}]
    for message in history.get("messages", []):
        messages.append({"role": message.get("role"), "content": message.get("message")})

    max_iterations = 5
    for iteration in range(max_iterations):
        is_final_iteration = (iteration == max_iterations - 1)

        completion_params = {
            "messages": messages,
            "model": "gpt-4.1-mini",
            "temperature": 0.5
        }

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
        else:
            completion_params["tools"] = inbuilt_tools + mcp_tools
        
        result = openai.complete(ctx, completion_params)
        if not result.get("choices", []):
            return "No response from the model"
        
        assistant_message = result["choices"][0]["message"]
        messages.append(assistant_message)
        
        has_tool_calls = assistant_message.get("tool_calls")
        if not has_tool_calls:
            break

        tool_messages = execute_tool_calls(ctx, assistant_message["tool_calls"])
        messages.extend(tool_messages)
    
    return messages[-1].get("content", "No content in the model response")