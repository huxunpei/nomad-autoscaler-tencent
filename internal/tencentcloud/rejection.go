package tencentcloud

import (
	"context"
	"errors"
)

// Only documented pre-execution rejections are listed. In particular a generic
// FailedOperation/InternalError/ResourcesSoldOut does not prove no side effect.
// Sources: AS error codes /377/30416, ModifyDesiredCapacity /377/20432,
// RemoveInstances /377/20431, ScaleOutInstances /377/56369.
// Future subcodes default to unknown.
func rejectedCode(action, code string) bool {
	if !oneOf(action, "ModifyDesiredCapacity", "RemoveInstances", "ScaleOutInstances") {
		return false
	}
	if rateLimitCode(code) {
		return true
	}
	switch code {
	case "AuthFailure.InvalidAuthorization", "AuthFailure.InvalidSecretId", "AuthFailure.MFAFailure",
		"AuthFailure.SecretIdNotFound", "AuthFailure.SignatureExpire", "AuthFailure.SignatureFailure",
		"AuthFailure.TokenFailure", "AuthFailure.UnauthorizedOperation", "UnauthorizedOperation",
		"InvalidAction", "MissingParameter", "UnknownParameter", "InvalidParameter.ActionNotFound",
		"InvalidParameterValue.InvalidAutoScalingGroupId", "ResourceNotFound.AutoScalingGroupNotFound":
		return true
	}
	if action == "ModifyDesiredCapacity" {
		return oneOf(code, "InvalidParameterValue.AssertDesiredCapacityFailed", "InvalidParameterValue.BaseCapacityTooLarge",
			"InvalidParameterValue.Range", "InvalidParameterValue.Size", "LimitExceeded.DesiredCapacityLimitExceeded",
			"ResourceUnavailable.AutoScalingGroupAbnormalStatus", "ResourceUnavailable.AutoScalingGroupDisabled")
	}
	if action == "ScaleOutInstances" {
		return oneOf(code, "InvalidParameterValue.Range", "LimitExceeded.DesiredCapacityLimitExceeded",
			"ResourceInsufficient.AutoScalingGroupAboveMaxSize", "ResourceUnavailable.AutoScalingGroupInActivity")
	}
	return oneOf(code, "InvalidParameterValue.InvalidInstanceId", "InvalidParameterValue.LimitExceeded",
		"ResourceInsufficient.AutoScalingGroupBelowMinSize", "ResourceInsufficient.InServiceInstanceBelowMinSize",
		"ResourceNotFound.InstancesNotInAutoScalingGroup", "ResourceUnavailable.AutoScalingGroupInActivity",
		"ResourceUnavailable.InstanceInOperation")
}

func mutationError(action string, err error, responseRequestID, activityID string) *Error {
	e := sanitize(action, err, true)
	if activityID == "" && requestPattern.MatchString(e.RequestID) &&
		(responseRequestID == "" || responseRequestID == e.RequestID) && rejectedCode(action, e.Code) &&
		!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		e.Unknown = false
	}
	return e
}

// IsRejectedWrite requires consistent typed evidence from this exact attempted
// mutation. An error string, missing acknowledgement or query RequestId cannot
// be used to release an unknown operation.
func IsRejectedWrite(r WriteResult, err error) bool {
	var e *Error
	return r.NotSubmitted == "" && errors.As(err, &e) && e != nil && r.Attempted && !r.Unknown && !e.Unknown &&
		r.ActivityID == "" && requestPattern.MatchString(r.RequestID) && r.RequestID == e.RequestID &&
		r.Action == e.Action && rejectedCode(e.Action, e.Code)
}

// NotSubmittedCode validates local provenance, never error text. Attempted=false
// alone is no proof. The transport source means an SDK attempt ran, but every
// native HTTP round trip failed before obtaining a usable connection.
func NotSubmittedCode(r WriteResult, err error) string {
	if err == nil || r.Unknown || r.RequestID != "" || r.ActivityID != "" ||
		!oneOf(r.Action, "ModifyDesiredCapacity", "RemoveInstances", "ScaleOutInstances") {
		return ""
	}
	var e *Error
	if errors.As(err, &e) && (e == nil || e.Unknown || e.RequestID != "") {
		return ""
	}
	switch r.NotSubmitted {
	case "credential-gate":
		if !r.Attempted {
			return "LocalCredentialGateNotSubmitted"
		}
	case "client-gate":
		if !r.Attempted && e != nil && e.Action == r.Action &&
			oneOf(e.Code, "InvalidMutation", "RequestCanceled", "RequestTimeout") {
			return "LocalClientGateNotSubmitted"
		}
	case "transport-before-connection":
		if r.Attempted && e != nil && e.Action == r.Action && e.Code == "LocalTransportNotSubmitted" {
			return e.Code
		}
	}
	return ""
}

func IsNotSubmittedWrite(r WriteResult, err error) bool { return NotSubmittedCode(r, err) != "" }
