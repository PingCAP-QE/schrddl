package pinolo

func BuildExcept(sql1, sql2 string) string {
	return "(" + sql1 + ")" + " except " + "(" + sql2 + ")"
}
