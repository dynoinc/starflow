package main

import (
	"context"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/packages/param"
)

type OpenAIRequest struct {
	Prompt      string  `json:"prompt"`
	Model       string  `json:"model,omitempty"`
	Temperature float64 `json:"temperature,omitempty"`
}

type OpenAIResponse struct {
	Completion string `json:"completion"`
}

func OpenAIComplete(ctx context.Context, req OpenAIRequest) (OpenAIResponse, error) {
	openaiClient := openai.NewClient()
	model := req.Model
	if model == "" {
		model = openai.ChatModelGPT4o
	}
	params := openai.ChatCompletionNewParams{
		Model:       model,
		Temperature: param.NewOpt(req.Temperature),
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.UserMessage(req.Prompt),
		},
	}
	resp, err := openaiClient.Chat.Completions.New(ctx, params)
	if err != nil {
		return OpenAIResponse{}, err
	}
	return OpenAIResponse{Completion: resp.Choices[0].Message.Content}, nil
}
