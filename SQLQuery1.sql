USE [Extracciones]
GO

/****** Object:  Table [dbo].[TA_FFMM_extracciones]    Script Date: 31/08/2020 17:01:08 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[TA_FFMM_extracciones](
	[id_extraccion] [bigint] IDENTITY(1,1) NOT NULL,
	[cartera] [varchar](100) NOT NULL,
	[periodo_extraccion] [varchar](100) NOT NULL,
	[extraido_en] [date] NOT NULL
) ON [PRIMARY]
GO


