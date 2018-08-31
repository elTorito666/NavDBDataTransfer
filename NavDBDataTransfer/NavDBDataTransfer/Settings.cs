namespace NavDBDataTransfer
{
    public static class Settings
    {
        //TODO: Make invisible
        public const string SourceCompany = "Source Mandant ";
        public const string TargetCompany = "Target Company";

        public const string SourceDBName = "NAV2009DB";
        public const string TargetDBName = "NAV2018DB";

        public const string ConnStringNav2018 = "Data Source=NAV18TEST;" +
                    "Initial Catalog=NAV2018DB;" +
                    "User id=*****;" +
                    "Password=*****;";
        public const string ConnStringNav2009 = "Data Source=NAV2009SQL;" +
                            "Initial Catalog=NAV2009DB;" +
                            "User id=*****;" +
                            "Password=****;";

        public const bool WithDeleteStatements = true;
        public const int SelectNRowsToInsert = 10; //0 = All 
    }
}
