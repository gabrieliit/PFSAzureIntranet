from ProducerServices.Transform import loan_delivery_datamap,loan_status_datamap,pending_loan_30_datamap,pending_loan_datamap,receipt_datamap
datamap_selector={
    "LoanDelivery":loan_delivery_datamap.TransformMap,
    "LoanStatus":loan_status_datamap.TransformMap,
    "PendingLoan_30":pending_loan_30_datamap.TransformMap,
    "PendingLoan":pending_loan_datamap.TransformMap,
    "Receipt":receipt_datamap.TransformMap
}