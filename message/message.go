package message

type Types interface {
	PaymentsHighValueTransferMessage
}

type PaymentsHighValueTransferMessage struct {
	Email  string
	From   string
	To     string
	Amount float32
}
