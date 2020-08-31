USE [Extracciones]
GO

/****** Object:  Table [dbo].[TA_AFP_extracciones]    Script Date: 31/08/2020 17:02:31 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[TA_AFP_extracciones](
	[id_extraccion] [bigint] IDENTITY(1,1) NOT NULL,
	[fondo] [varchar](100) NOT NULL,
	[periodo_extraccion] [varchar](100) NOT NULL,
	[extraido_en] [date] NOT NULL,
 CONSTRAINT [PK_TA_AFP_extracciones] PRIMARY KEY CLUSTERED 
(
	[id_extraccion] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


