// Code generated by MockGen. DO NOT EDIT.
// Source: kafko (interfaces: BackoffStrategy)
//
// Generated by this command:
//
//	mockgen -destination=./mocks/mock_backoff_strategy.go -package=mocks kafko BackoffStrategy
//
// Package mocks is a generated GoMock package.
package mocks

import (
	context "context"
	reflect "reflect"

	gomock "go.uber.org/mock/gomock"
)

// MockBackoffStrategy is a mock of BackoffStrategy interface.
type MockBackoffStrategy struct {
	ctrl     *gomock.Controller
	recorder *MockBackoffStrategyMockRecorder
}

// MockBackoffStrategyMockRecorder is the mock recorder for MockBackoffStrategy.
type MockBackoffStrategyMockRecorder struct {
	mock *MockBackoffStrategy
}

// NewMockBackoffStrategy creates a new mock instance.
func NewMockBackoffStrategy(ctrl *gomock.Controller) *MockBackoffStrategy {
	mock := &MockBackoffStrategy{ctrl: ctrl}
	mock.recorder = &MockBackoffStrategyMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockBackoffStrategy) EXPECT() *MockBackoffStrategyMockRecorder {
	return m.recorder
}

// Wait mocks base method.
func (m *MockBackoffStrategy) Wait(arg0 context.Context) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Wait", arg0)
}

// Wait indicates an expected call of Wait.
func (mr *MockBackoffStrategyMockRecorder) Wait(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Wait", reflect.TypeOf((*MockBackoffStrategy)(nil).Wait), arg0)
}
