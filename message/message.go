package message

type Types interface {
	PaymentsHighValueTransferMessage
}

type PaymentsHighValueTransferMessage struct {
	Email  string  `json:"email"`
	From   string  `json:"from"`
	To     string  `json:"to"`
	Amount float32 `json:"amount"`
}
