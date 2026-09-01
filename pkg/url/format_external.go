package url

import neturl "net/url"

func (u *URL) formatExternal() string {
	return (&neturl.URL{
		Scheme: "external",
		Host:   u.Host,
	}).String()
}
