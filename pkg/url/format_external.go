package url

import neturl "net/url"

func (u *URL) formatExternal() string {
	query := neturl.Values{}
	query.Set(externalPathQueryParameter, u.Path)
	query.Set(ExternalRootGrantIdentifierParameter, u.Parameters[ExternalRootGrantIdentifierParameter])
	return (&neturl.URL{
		Scheme:   "external",
		Host:     u.Host,
		RawQuery: query.Encode(),
	}).String()
}
