package url

import neturl "net/url"

func (u *URL) formatHappier() string {
	query := neturl.Values{}
	query.Set(happierPathQueryParameter, u.Path)
	query.Set(HappierRootGrantIdentifierParameter, u.Parameters[HappierRootGrantIdentifierParameter])
	return (&neturl.URL{
		Scheme:   "happier",
		Host:     u.Host,
		RawQuery: query.Encode(),
	}).String()
}
