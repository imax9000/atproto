import { Server } from '../../../../lexicon'
import AppContext from '../../../../context'
import { authPassthru } from '../../../proxy'

export default function (server: Server, ctx: AppContext) {
  server.com.atproto.admin.deleteCommunicationTemplate({
    auth: ctx.authVerifier.role,
    handler: async ({ req, input }) => {
      await ctx.moderationAgent.com.atproto.admin.deleteCommunicationTemplate(
        input.body,
        authPassthru(req, true),
      )
    },
  })
}
