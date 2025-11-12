CREATE TABLE [dbo].[ID_repository_logindetails](
	[Sl_no] [int] IDENTITY(1,1) NOT NULL,
	[RequestID] [varchar](50) NULL,
	[ServerName] [varchar](100) NOT NULL,
	[LoginName] [varchar](100) NOT NULL,
	[UserName] [varchar](100) NOT NULL,
	[Password] [varchar](255) NOT NULL,
	[DatabaseName] [varchar](100) NOT NULL,
	[Role] [varchar](50) NULL,
	[Application] [varchar](100) NOT NULL,
	[CreatedDate] [datetime] NOT NULL,
	[Reason] [varchar](max) NOT NULL,
	[created_by] [varchar](250) NULL
)
GO

ALTER TABLE [dbo].[ID_repository_logindetails] ADD  DEFAULT (getdate()) FOR [CreatedDate]
GO